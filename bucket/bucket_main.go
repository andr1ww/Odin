package bucket

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/andr1ww/odin/database"
	"github.com/andr1ww/odin/internal/compression"
	"github.com/andr1ww/odin/internal/indexing"
	"github.com/andr1ww/odin/internal/reflection"
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

	if indexing.HasIndex(bucketName) && len(criteria) == 1 {
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

	sampleEntity := constructor()
	entityType := reflect.TypeOf(sampleEntity).Elem()
	matcher := reflection.GetFieldMatcher(entityType)

	var results []interface{}
	const numWorkers = 4
	workChan := make(chan []byte, numWorkers*2)
	resultChan := make(chan []interface{}, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localResults := make([]interface{}, 0, 100)

			for data := range workChan {
				if len(data) == 0 {
					continue
				}

				var actualData []byte
				if len(data) > 0 && (data[0] == 0 || data[0] == 1) {
					if data[0] == 1 {
						gzReader, err := gzip.NewReader(bytes.NewReader(data[1:]))
						if err != nil {
							actualData = data
						} else {
							var buf bytes.Buffer
							if _, err := buf.ReadFrom(gzReader); err != nil {
								actualData = data
							} else {
								actualData = buf.Bytes()
							}
							gzReader.Close()
						}
					} else {
						actualData = data[1:]
					}
				} else {
					actualData = compression.DecompressData(data)
				}

				entity := constructor()
				if err := json.Unmarshal(actualData, entity); err != nil {
					continue
				}

				if reflection.MatchesCriteria(entity, criteria, matcher) {
					localResults = append(localResults, entity)
				}
			}

			resultChan <- localResults
		}()
	}

	go func() {
		defer close(workChan)
		db.ForEach(bucketName, func(_, v []byte) error {
			dataCopy := make([]byte, len(v))
			copy(dataCopy, v)
			select {
			case workChan <- dataCopy:
			case <-time.After(5 * time.Second):
				return fmt.Errorf("timeout writing to work channel")
			}
			return nil
		})
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	timeout := time.After(30 * time.Second)
	for {
		select {
		case localResults, ok := <-resultChan:
			if !ok {
				return results, nil
			}
			results = append(results, localResults...)
		case <-timeout:
			return nil, fmt.Errorf("timeout waiting for results")
		}
	}
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
