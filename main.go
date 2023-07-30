package main

import (
	"context"
	"crypto/sha1"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/sync/semaphore"
	"gopkg.in/yaml.v3"

	"fmt"
)

type BucketSpec struct {
	Name    string `yaml:"name"`
	Region  string `yaml:"region"`
	session *session.Session
	ctx     context.Context
}

type Config struct {
	SourceBuckets     []BucketSpec `yaml:"source_buckets"`
	DestinationBucket BucketSpec   `yaml:"destination_bucket"`
}

type S3URI string
type ContentHash string

type Deduplicator struct {
	Items map[S3URI]ContentHash
	lock  sync.Mutex
}

func NewDeduplicator() *Deduplicator {
	d := &Deduplicator{
		Items: make(map[S3URI]ContentHash, 1000),
	}

	yamlFile, err := os.ReadFile("cache.yaml")
	if err != nil {
		fmt.Printf("Error reading cache.yaml: %s\n", err)
		return d
	}
	yaml.Unmarshal(yamlFile, &d)
	return d
}

func (d *Deduplicator) flush() {
	// write to cache.yaml
	fmt.Printf("Flushing %d items to cache.yaml\n", len(d.Items))
	d.lock.Lock()
	out, err := yaml.Marshal(d)
	println(out)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile("cache_tmp.yaml", out, 0644)
	if err != nil {
		panic(err)
	}
	d.lock.Unlock()
	os.Rename("cache_tmp.yaml", "cache.yaml")
}

func (d *Deduplicator) HasItem(uri S3URI) bool {
	d.lock.Lock()
	_, ok := d.Items[uri]
	d.lock.Unlock()
	return ok
}

func (d *Deduplicator) AddContentHash(hash ContentHash, uri S3URI) {
	d.lock.Lock()
	d.Items[uri] = hash
	d.lock.Unlock()

	if len(d.Items)%50 == 0 {
		d.flush()
	}
}

func (d *Deduplicator) RemoveItem(uri S3URI) {
	d.lock.Lock()
	delete(d.Items, uri)
	d.lock.Unlock()
}

func (d *Deduplicator) Deduplicate() map[ContentHash]S3URI {
	ddItems := make(map[ContentHash]S3URI, len(d.Items))
	d.lock.Lock()
	for k, v := range d.Items {
		ddItems[v] = k
	}
	d.lock.Unlock()
	return ddItems
}

var dd = NewDeduplicator()
var globalOpSemaphore = semaphore.NewWeighted(50)

func LoadYamlConfig() Config {
	// load config.yaml
	conf := Config{}
	yamlFile, err := os.ReadFile("config.yaml")
	if err != nil {
		panic(err)
	}
	yaml.Unmarshal(yamlFile, &conf)
	return conf
}

func (b *BucketSpec) hashContents(object *s3.Object) {
	s3uri := S3URI(fmt.Sprintf("s3://%s/%s", b.Name, *object.Key))
	if dd.HasItem(s3uri) {
		fmt.Printf("Skipping cached %s\n", s3uri)
		return
	}

	globalOpSemaphore.Acquire(b.ctx, 1)
	defer globalOpSemaphore.Release(1)
	client := s3.New(b.session)
	resp, err := client.GetObjectWithContext(b.ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.Name),
		Key:    object.Key,
	})
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	// generate SHA256 hash of contents
	h := sha1.New()
	if *object.Size > 100_000_000 {
		// custom hash for large file to avoid downloading the entire thing
		lbody := io.LimitReader(resp.Body, 1_000_000)
		if _, err := io.Copy(h, lbody); err != nil {
			panic(err)
		}
		h.Write([]byte(fmt.Sprintf("TRUNC%d", *object.Size)))
	} else {
		if _, err := io.Copy(h, resp.Body); err != nil {
			panic(err)
		}
	}
	fmt.Printf("%s => %x\n", s3uri, h.Sum(nil))
	dd.AddContentHash(ContentHash(fmt.Sprintf("%x", h.Sum(nil))), s3uri)
}

func (b *BucketSpec) Setup() {
	fmt.Println("Setting up bucket:", b.Name)
	b.ctx = context.Background()
	b.session, _ = session.NewSession(aws.NewConfig().WithRegion(b.Region))
	_, err := b.session.Config.Credentials.Get()
	if err != nil {
		panic(err)
	}
}

func (b *BucketSpec) processSourceBucket() {
	client := s3.New(b.session)
	// list all files in the bucket
	var params = &s3.ListObjectsV2Input{
		Bucket:  aws.String(b.Name), // put a valid bucket name here
		MaxKeys: aws.Int64(50),
	}

	wg := sync.WaitGroup{}
	objCounter := 0
	for {
		// send the request
		resp, err := client.ListObjectsV2(params)
		if err != nil {
			panic(err)
		}

		// list outputs
		for _, object := range resp.Contents {
			wg.Add(1)
			objCounter++
			go func(s3o *s3.Object) {
				defer wg.Done()
				b.hashContents(s3o)
			}(object)
		}

		// if results truncated, keep paginating
		if *resp.IsTruncated {
			fmt.Println("Results truncated... paginating.")
			fmt.Println("Next Marker:", *resp.NextContinuationToken)
			params.SetContinuationToken(*resp.NextContinuationToken)
			continue
		}
		break
	}
	fmt.Printf("[%s] Waiting for all s3 object %d goroutines to finish\n", b.Name, objCounter)
	wg.Wait()
}

func copyToDestination(destination *BucketSpec, source S3URI) {
	globalOpSemaphore.Acquire(context.Background(), 1)
	defer globalOpSemaphore.Release(1)
	fmt.Println("Copying", source, "to", destination.Name)
	session := session.Must(
		session.NewSession(aws.NewConfig().WithRegion(destination.Region)),
	)
	client := s3.New(session)
	outputKey := strings.TrimPrefix(string(source), "s3://")
	outputParts := strings.SplitN(outputKey, "/", 2)

	client.CopyObject(&s3.CopyObjectInput{
		CopySource: aws.String(
			url.QueryEscape(strings.TrimPrefix(string(source), "s3://")),
		),
		Bucket: aws.String(destination.Name),
		Key:    aws.String(outputParts[1]),
	})
}

func main() {
	config := LoadYamlConfig()

	wg := sync.WaitGroup{}
	for _, sourceBucket := range config.SourceBuckets {
		sourceBucket := sourceBucket
		wg.Add(1)
		go func() {
			defer wg.Done()
			sourceBucket.Setup()
			sourceBucket.processSourceBucket()
		}()
	}
	wg.Wait()
	dd.flush()

	// deduplicate
	ddItems := dd.Deduplicate()

	fmt.Printf("Found %d unique items from %d total items\n", len(ddItems), len(dd.Items))
	for _, originURI := range ddItems {
		wg.Add(1)
		uri := originURI
		go func() {
			copyToDestination(&config.DestinationBucket, uri)
			wg.Done()
		}()
	}
	wg.Wait()
}
