package indexing

import (
	"reflect"
	"strings"
	"sync"

	"github.com/andr1ww/odin/internal/reflection"
)

var bucketIndexes = make(map[string]map[string]map[interface{}][]string)
var indexMutex sync.RWMutex

func UpdateIndex(bucketName, key string, entity interface{}) {
	indexMutex.Lock()
	defer indexMutex.Unlock()

	if _, exists := bucketIndexes[bucketName]; !exists {
		bucketIndexes[bucketName] = make(map[string]map[interface{}][]string)
	}

	entityValue := reflect.ValueOf(entity)
	if entityValue.Kind() == reflect.Ptr {
		entityValue = entityValue.Elem()
	}
	entityType := entityValue.Type()
	matcher := reflection.GetFieldMatcher(entityType)

	for i := 0; i < entityType.NumField(); i++ {
		field := entityType.Field(i)
		fieldName := field.Name

		jsonTag := field.Tag.Get("json")
		if jsonTag != "" {
			if comma := strings.Index(jsonTag, ","); comma != -1 {
				jsonTag = jsonTag[:comma]
			}
			if jsonTag != "" && jsonTag != "-" {
				fieldName = jsonTag
			}
		}

		if _, exists := bucketIndexes[bucketName][fieldName]; !exists {
			bucketIndexes[bucketName][fieldName] = make(map[interface{}][]string)
		}

		if fieldValue, found := matcher.GetFieldValue(entityValue, fieldName); found {
			if !isHashable(fieldValue) {
				continue
			}

			fieldIndex := bucketIndexes[bucketName][fieldName]
			keys := fieldIndex[fieldValue]
			keyExists := false
			for _, k := range keys {
				if k == key {
					keyExists = true
					break
				}
			}
			if !keyExists {
				fieldIndex[fieldValue] = append(keys, key)
			}
		}
	}
}

func RemoveFromIndex(bucketName, key string, entity interface{}) {
	indexMutex.Lock()
	defer indexMutex.Unlock()

	if _, exists := bucketIndexes[bucketName]; !exists {
		return
	}

	entityValue := reflect.ValueOf(entity)
	if entityValue.Kind() == reflect.Ptr {
		entityValue = entityValue.Elem()
	}
	entityType := entityValue.Type()
	matcher := reflection.GetFieldMatcher(entityType)

	for i := 0; i < entityType.NumField(); i++ {
		field := entityType.Field(i)
		fieldName := field.Name

		jsonTag := field.Tag.Get("json")
		if jsonTag != "" {
			if comma := strings.Index(jsonTag, ","); comma != -1 {
				jsonTag = jsonTag[:comma]
			}
			if jsonTag != "" && jsonTag != "-" {
				fieldName = jsonTag
			}
		}

		if fieldIndex, exists := bucketIndexes[bucketName][fieldName]; exists {
			if fieldValue, found := matcher.GetFieldValue(entityValue, fieldName); found {
				if !isHashable(fieldValue) {
					continue
				}

				if keys, exists := fieldIndex[fieldValue]; exists {
					for i, k := range keys {
						if k == key {
							fieldIndex[fieldValue] = append(keys[:i], keys[i+1:]...)
							break
						}
					}
					if len(fieldIndex[fieldValue]) == 0 {
						delete(fieldIndex, fieldValue)
					}
				}
			}
		}
	}
}

func GetIndexedKeys(bucketName, field string, value interface{}) ([]string, bool) {
	indexMutex.RLock()
	defer indexMutex.RUnlock()

	bucketIndex, hasIndex := bucketIndexes[bucketName]
	if !hasIndex {
		return nil, false
	}

	fieldIndex, exists := bucketIndex[field]
	if !exists {
		return nil, false
	}

	keys, keyExists := fieldIndex[value]
	if !keyExists {
		return nil, false
	}

	keysCopy := make([]string, len(keys))
	copy(keysCopy, keys)
	return keysCopy, true
}

func HasIndex(bucketName string) bool {
	indexMutex.RLock()
	defer indexMutex.RUnlock()
	_, exists := bucketIndexes[bucketName]
	return exists
}

func isHashable(v interface{}) bool {
	if v == nil {
		return true
	}

	return isTypeHashable(reflect.TypeOf(v))
}

func isTypeHashable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Slice, reflect.Map, reflect.Func:
		return false
	case reflect.Ptr:
		return isTypeHashable(t.Elem())
	case reflect.Array:
		return isTypeHashable(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if !isTypeHashable(field.Type) {
				return false
			}
		}
		return true
	case reflect.Interface:
		return false
	case reflect.Chan:
		return false
	default:
		return true
	}
}
