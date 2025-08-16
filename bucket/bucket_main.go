package bucket

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/andr1ww/odin/database"
	"github.com/andr1ww/odin/internal/compression"
	"github.com/andr1ww/odin/internal/indexing"
	"github.com/andr1ww/odin/internal/reflection"
)

var (
	gzipReaderPool = sync.Pool{
		New: func() interface{} {
			return &gzip.Reader{}
		},
	}
	workerPool = sync.Pool{
		New: func() interface{} {
			slice := make([]interface{}, 0, 200)
			return &slice
		},
	}
	bufferPool = sync.Pool{
		New: func() interface{} {
			return &bytes.Buffer{}
		},
	}
	dataBufferPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 0, 4096)
			return &buf
		},
	}
	fieldMatcherCache = sync.Map{}
)

func Find(bucketName string, id string, entity interface{}) error {
	dbName, err := reflection.GetBucketDatabase(entity)
	if err != nil {
		return err
	}
	return FindInDatabase(dbName, bucketName, id, entity)
}

func FindWhere(bucketName string, criteria map[string]interface{}, constructor func() interface{}) ([]interface{}, error) {
	entity := constructor()
	dbName, err := reflection.GetBucketDatabase(entity)
	if err != nil {
		return nil, err
	}
	return FindWhereInDatabase(dbName, bucketName, criteria, constructor)
}

func Create(entity interface{}) error {
	dbName, err := reflection.GetBucketDatabase(entity)
	if err != nil {
		return err
	}

	return CreateInDatabase(dbName, entity)
}

func FindAll(bucketName string, constructor func() interface{}) ([]interface{}, error) {
	entity := constructor()
	dbName, err := reflection.GetBucketDatabase(entity)
	if err != nil {
		return nil, err
	}
	return FindAllInDatabase(dbName, bucketName, constructor)
}

func FindInDatabase(dbName, bucketName, id string, entity interface{}) error {
	db, err := database.GetNamed(dbName)
	if err != nil {
		return err
	}

	return db.Get(bucketName, id, entity)
}

func FindWhereInDatabase(dbName, bucketName string, criteria map[string]interface{}, constructor func() interface{}) ([]interface{}, error) {
	db, err := database.GetNamed(dbName)
	if err != nil {
		return nil, err
	}

	if indexing.HasIndex(bucketName) {
		if len(criteria) == 1 {
			for field, value := range criteria {
				if keys, found := indexing.GetIndexedKeys(bucketName, field, value); found {
					results := make([]interface{}, 0, len(keys))
					for _, key := range keys {
						entity := constructor()
						if err := db.Get(bucketName, key, entity); err == nil {
							results = append(results, entity)
						}
					}
					return results, nil
				}
			}
		}

		if len(criteria) > 1 {
			var candidateKeys []string
			firstField := true

			for field, value := range criteria {
				if keys, found := indexing.GetIndexedKeys(bucketName, field, value); found {
					if firstField {
						candidateKeys = keys
						firstField = false
					} else {
						candidateKeys = intersectStringSlices(candidateKeys, keys)
						if len(candidateKeys) == 0 {
							return []interface{}{}, nil
						}
					}
				} else {
					candidateKeys = nil
					break
				}
			}

			if candidateKeys != nil {
				results := make([]interface{}, 0, len(candidateKeys))
				for _, key := range candidateKeys {
					entity := constructor()
					if err := db.Get(bucketName, key, entity); err == nil {
						results = append(results, entity)
					}
				}
				return results, nil
			}
		}
	}

	sampleEntity := constructor()
	entityType := reflect.TypeOf(sampleEntity).Elem()

	var matcher *reflection.FieldMatcher
	if cached, ok := fieldMatcherCache.Load(entityType); ok {
		matcher = cached.(*reflection.FieldMatcher)
	} else {
		matcher = reflection.GetFieldMatcher(entityType)
		fieldMatcherCache.Store(entityType, matcher)
	}

	numWorkers := runtime.NumCPU()
	if numWorkers > 6 {
		numWorkers = 6
	}

	workChan := make(chan []byte, numWorkers*2)
	resultChan := make(chan []interface{}, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localResultsPtr := workerPool.Get().(*[]interface{})
			localResults := *localResultsPtr
			defer func() {
				*localResultsPtr = (*localResultsPtr)[:0]
				workerPool.Put(localResultsPtr)
			}()

			buffer := bufferPool.Get().(*bytes.Buffer)
			defer func() {
				buffer.Reset()
				bufferPool.Put(buffer)
			}()

			dataBufferPtr := dataBufferPool.Get().(*[]byte)
			defer func() {
				*dataBufferPtr = (*dataBufferPtr)[:0]
				dataBufferPool.Put(dataBufferPtr)
			}()

			for data := range workChan {
				if len(data) == 0 {
					continue
				}

				var actualData []byte
				if len(data) > 0 && (data[0] == 0 || data[0] == 1) {
					if data[0] == 1 {
						reader := gzipReaderPool.Get().(*gzip.Reader)
						if err := reader.Reset(bytes.NewReader(data[1:])); err == nil {
							*dataBufferPtr = (*dataBufferPtr)[:0]

							chunk := make([]byte, 4096)
							for {
								n, err := reader.Read(chunk)
								if n > 0 {
									*dataBufferPtr = append(*dataBufferPtr, chunk[:n]...)
								}
								if err == io.EOF {
									break
								}
								if err != nil {
									actualData = data
									break
								}
							}

							if actualData == nil {
								actualData = make([]byte, len(*dataBufferPtr))
								copy(actualData, *dataBufferPtr)
							}
							reader.Close()
						} else {
							actualData = data
						}
						gzipReaderPool.Put(reader)
					} else {
						actualData = data[1:]
					}
				} else {
					actualData = compression.DecompressData(data)
				}

				entity := constructor()

				buffer.Reset()
				buffer.Write(actualData)
				decoder := json.NewDecoder(buffer)

				if err := decoder.Decode(entity); err != nil {
					continue
				}

				if reflection.MatchesCriteria(entity, criteria, matcher) {
					localResults = append(localResults, entity)
				}
			}

			if len(localResults) > 0 {
				resultCopy := make([]interface{}, len(localResults))
				copy(resultCopy, localResults)
				resultChan <- resultCopy
			} else {
				resultChan <- nil
			}
		}()
	}

	go func() {
		defer close(workChan)
		db.ForEach(bucketName, func(_, v []byte) error {
			dataCopy := make([]byte, len(v))
			copy(dataCopy, v)
			select {
			case workChan <- dataCopy:
			case <-time.After(10 * time.Second):
				return fmt.Errorf("timeout writing to work channel")
			}
			return nil
		})
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var results []interface{}
	timeout := time.After(60 * time.Second)
	for {
		select {
		case localResults, ok := <-resultChan:
			if !ok {
				return results, nil
			}
			if localResults != nil {
				results = append(results, localResults...)
			}
		case <-timeout:
			return nil, fmt.Errorf("timeout waiting for results")
		}
	}
}

func intersectStringSlices(a, b []string) []string {
	if len(a) == 0 || len(b) == 0 {
		return []string{}
	}

	if len(b) < len(a) {
		a, b = b, a
	}

	bMap := make(map[string]bool, len(b))
	for _, item := range b {
		bMap[item] = true
	}

	var result []string
	for _, item := range a {
		if bMap[item] {
			result = append(result, item)
			delete(bMap, item)
		}
	}

	return result
}

func CreateInDatabase(dbName string, entity interface{}) error {
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if field.Type().Name() == "Bucket" {
			bucket := field.Addr().Interface().(*Bucket)
			bucket.SetDatabase(dbName)
			bucket.BeforeSave()
			return bucket.SaveToDatabase(dbName, entity)
		}
	}

	db, err := database.GetNamed(dbName)
	if err != nil {
		return err
	}

	bucketName, err := reflection.GetBucketName(entity)
	if err != nil {
		return err
	}

	var id string
	idField := val.FieldByName("ID")
	if idField.IsValid() {
		id = idField.String()
	} else {
		for i := 0; i < val.NumField(); i++ {
			field := val.Type().Field(i)
			if strings.HasSuffix(field.Name, "ID") {
				id = val.Field(i).String()
				break
			}
		}
	}

	if id == "" {
		return errors.New("could not find ID field")
	}

	indexing.UpdateIndex(bucketName, id, entity)
	return db.Put(bucketName, id, entity)
}

func FindAllInDatabase(dbName, bucketName string, constructor func() interface{}) ([]interface{}, error) {
	db, err := database.GetNamed(dbName)
	if err != nil {
		return nil, err
	}

	return db.GetAll(bucketName, constructor)
}
