package reflection

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/andr1ww/odin/internal/logger"
	bolt "go.etcd.io/bbolt"
)

var bucketNameCache = sync.Map{}

type FieldMatcher struct {
	FieldMap map[string]int
	JsonMap  map[string]int
	Fields   []reflect.StructField
}

var matcherCache = sync.Map{}

func GetFieldMatcher(typ reflect.Type) *FieldMatcher {
	if matcher, exists := matcherCache.Load(typ); exists {
		return matcher.(*FieldMatcher)
	}

	numFields := typ.NumField()
	matcher := &FieldMatcher{
		FieldMap: make(map[string]int, numFields),
		JsonMap:  make(map[string]int, numFields),
		Fields:   make([]reflect.StructField, numFields),
	}

	for i := 0; i < numFields; i++ {
		field := typ.Field(i)
		matcher.Fields[i] = field
		matcher.FieldMap[field.Name] = i

		jsonTag := field.Tag.Get("json")
		if jsonTag != "" {
			if comma := strings.IndexByte(jsonTag, ','); comma != -1 {
				jsonTag = jsonTag[:comma]
			}
			if jsonTag != "" && jsonTag != "-" {
				matcher.JsonMap[jsonTag] = i
			}
		}
	}

	if cached, loaded := matcherCache.LoadOrStore(typ, matcher); loaded {
		return cached.(*FieldMatcher)
	}
	return matcher
}

func (fm *FieldMatcher) GetFieldValue(entityValue reflect.Value, key string) (interface{}, bool) {
	if idx, exists := fm.JsonMap[key]; exists {
		return entityValue.Field(idx).Interface(), true
	}
	if idx, exists := fm.FieldMap[key]; exists {
		return entityValue.Field(idx).Interface(), true
	}
	return nil, false
}

func MatchesCriteria(entity interface{}, criteria map[string]interface{}, matcher *FieldMatcher) bool {
	entityValue := reflect.ValueOf(entity)
	if entityValue.Kind() == reflect.Ptr {
		entityValue = entityValue.Elem()
	}

	for key, expectedValue := range criteria {
		var fieldValue interface{}
		var found bool

		if idx, exists := matcher.JsonMap[key]; exists {
			fieldValue = entityValue.Field(idx).Interface()
			found = true
		} else if idx, exists := matcher.FieldMap[key]; exists {
			fieldValue = entityValue.Field(idx).Interface()
			found = true
		}

		if !found {
			return false
		}

		if fieldValue != expectedValue {
			if !reflect.DeepEqual(fieldValue, expectedValue) {
				return false
			}
		}
	}
	return true
}

func GetBucketName(v interface{}) (string, error) {
	if v == nil {
		return "", errors.New("nil value provided")
	}

	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return "", fmt.Errorf("expected struct, got %s", val.Kind())
	}

	typ := val.Type()
	if cached, exists := bucketNameCache.Load(typ); exists {
		return cached.(string), nil
	}

	var bucketName string
	found := false
	numFields := typ.NumField()

	for i := 0; i < numFields; i++ {
		field := typ.Field(i)

		if field.Type.Name() == "Bucket" {
			if name, ok := field.Tag.Lookup("bucket"); ok {
				bucketName = name
				found = true
				break
			}
		}

		if name, ok := field.Tag.Lookup("bucket"); ok {
			bucketName = name
			found = true
			break
		}
	}

	if !found {
		structName := typ.Name()
		if len(structName) > 6 && structName[len(structName)-6:] == "Entity" {
			structName = structName[:len(structName)-6]
		}
		bucketName = structName
	}

	if cached, loaded := bucketNameCache.LoadOrStore(typ, bucketName); loaded {
		return cached.(string), nil
	}
	return bucketName, nil
}

func GetBucketDatabase(v interface{}) (string, error) {
	if v == nil {
		return "", errors.New("nil value provided")
	}

	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return "", fmt.Errorf("expected struct, got %s", val.Kind())
	}

	typ := val.Type()
	numFields := typ.NumField()

	for i := 0; i < numFields; i++ {
		field := typ.Field(i)

		if field.Type.Name() == "Bucket" {
			if dbName, ok := field.Tag.Lookup("database"); ok {
				return dbName, nil
			}
		}

		if dbName, ok := field.Tag.Lookup("database"); ok {
			return dbName, nil
		}
	}

	return "", errors.New("no database tag found")
}

func FindAndInitBuckets(db *bolt.DB, dbName string) error {
	buckets, err := findBucketsForDatabase(dbName)
	if err != nil {
		return fmt.Errorf("failed to find buckets: %w", err)
	}

	if len(buckets) == 0 {
		logger.Warning("no buckets found for database %s", dbName)
		return nil
	}

	return db.Update(func(tx *bolt.Tx) error {
		for _, bucketName := range buckets {
			bucket := tx.Bucket([]byte(bucketName))
			if bucket == nil {
				logger.Warning("creating bucket: %s", bucketName)
				_, err := tx.CreateBucket([]byte(bucketName))
				if err != nil {
					return fmt.Errorf("create %s bucket: %w", bucketName, err)
				}
			}
		}
		return nil
	})
}

var ignoredStructs = map[string]bool{
	"Bucket": true,
	"DB":     true,
}

func findBucketsForDatabase(dbName string) ([]string, error) {
	buckets := make(map[string]struct{})

	err := scanForBuckets(".", buckets, dbName)
	if err != nil {
		logger.Error("scanning .go files for buckets: %v", err)
	}

	result := make([]string, 0, len(buckets))
	for bucket := range buckets {
		if ignoredStructs[bucket] {
			continue
		}
		result = append(result, bucket)
	}

	return result, nil
}

func scanForBuckets(rootDir string, buckets map[string]struct{}, targetDB string) error {
	return filepath.Walk(rootDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if strings.Contains(path, "/vendor/") ||
			strings.Contains(path, "/.git/") ||
			strings.HasSuffix(path, "_test.go") {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if !info.IsDir() && strings.HasSuffix(path, ".go") {
			content, err := os.ReadFile(path)
			if err != nil {
				return nil
			}

			contentStr := string(content)
			if !strings.Contains(contentStr, ".Bucket") || !strings.Contains(contentStr, targetDB) {
				return nil
			}

			lines := strings.Split(contentStr, "\n")

			for _, line := range lines {
				line = strings.TrimSpace(line)
				if !strings.Contains(line, ".Bucket") || !strings.Contains(line, "`") {
					continue
				}

				var bucketName, dbName string

				if bucketStart := strings.Index(line, "bucket:\""); bucketStart != -1 {
					bucketStart += 8
					if bucketEnd := strings.Index(line[bucketStart:], "\""); bucketEnd != -1 {
						bucketName = line[bucketStart : bucketStart+bucketEnd]
					}
				}

				if dbStart := strings.Index(line, "database:\""); dbStart != -1 {
					dbStart += 10
					if dbEnd := strings.Index(line[dbStart:], "\""); dbEnd != -1 {
						dbName = line[dbStart : dbStart+dbEnd]
					}
				}

				if bucketName != "" && dbName == targetDB {
					buckets[bucketName] = struct{}{}
				}
			}
		}
		return nil
	})
}
