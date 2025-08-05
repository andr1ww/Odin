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

var bucketNameCache = make(map[reflect.Type]string)
var bucketNameMutex sync.RWMutex

type FieldMatcher struct {
	FieldMap map[string]int
	JsonMap  map[string]int
}

var matcherCache = make(map[reflect.Type]*FieldMatcher)
var matcherMutex sync.RWMutex

func GetFieldMatcher(typ reflect.Type) *FieldMatcher {
	matcherMutex.RLock()
	if matcher, exists := matcherCache[typ]; exists {
		matcherMutex.RUnlock()
		return matcher
	}
	matcherMutex.RUnlock()

	matcher := &FieldMatcher{
		FieldMap: make(map[string]int),
		JsonMap:  make(map[string]int),
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		matcher.FieldMap[field.Name] = i

		jsonTag := field.Tag.Get("json")
		if jsonTag != "" {
			if comma := strings.Index(jsonTag, ","); comma != -1 {
				jsonTag = jsonTag[:comma]
			}
			if jsonTag != "" && jsonTag != "-" {
				matcher.JsonMap[jsonTag] = i
			}
		}
	}

	matcherMutex.Lock()
	matcherCache[typ] = matcher
	matcherMutex.Unlock()

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
		fieldValue, found := matcher.GetFieldValue(entityValue, key)
		if !found || !reflect.DeepEqual(fieldValue, expectedValue) {
			return false
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
	bucketNameMutex.RLock()
	if cached, exists := bucketNameCache[typ]; exists {
		bucketNameMutex.RUnlock()
		return cached, nil
	}
	bucketNameMutex.RUnlock()

	var bucketName string
	found := false

	for i := 0; i < typ.NumField(); i++ {
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

	bucketNameMutex.Lock()
	bucketNameCache[typ] = bucketName
	bucketNameMutex.Unlock()

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
	for i := 0; i < typ.NumField(); i++ {
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

func findBucketsForDatabase(dbName string) ([]string, error) {
	buckets := make(map[string]struct{})

	err := scanForBuckets(".", buckets, dbName)
	if err != nil {
		logger.Error("scanning .go files for buckets: %v", err)
	}

	result := make([]string, 0, len(buckets))
	for bucket := range buckets {
		var ignoredStructs = map[string]bool{
			"Bucket": true,
			"DB":     true,
		}

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
			lines := strings.Split(contentStr, "\n")

			for _, line := range lines {
				line = strings.TrimSpace(line)
				if strings.Contains(line, ".Bucket") && strings.Contains(line, "`") {
					bucketName := ""
					dbName := ""

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

					if bucketName != "" && dbName != "" && dbName == targetDB {
						buckets[bucketName] = struct{}{}
					}
				}
			}
		}
		return nil
	})
}
