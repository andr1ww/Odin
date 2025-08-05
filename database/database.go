package database

import (
	err "errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/andr1ww/odin/errors"
	"github.com/andr1ww/odin/internal/compression"
	"github.com/andr1ww/odin/internal/logger"
	"github.com/andr1ww/odin/internal/reflection"
	jsoniter "github.com/json-iterator/go"
	bolt "go.etcd.io/bbolt"
)

var js = jsoniter.ConfigCompatibleWithStandardLibrary

type DB struct {
	*bolt.DB
	name string
}

func openDatabase(name, dbPath string) (*DB, error) {
	boltDB, err := bolt.Open(dbPath, 0600, &bolt.Options{
		Timeout:         10 * time.Second,
		InitialMmapSize: 1000 * 1024 * 1024,
		PageSize:        8096,
		NoSync:          false,
		NoFreelistSync:  true,
		FreelistType:    bolt.FreelistMapType,
		NoGrowSync:      true,
		MmapFlags:       0,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to open database %s: %w", name, err)
	}

	err = boltDB.Update(func(tx *bolt.Tx) error {
		return nil
	})
	if err != nil {
		boltDB.Close()
		return nil, err
	}

	if err := reflection.FindAndInitBuckets(boltDB, name); err != nil {
		boltDB.Close()
		return nil, err
	}

	return &DB{DB: boltDB, name: name}, nil
}

func (db *DB) GetName() string {
	return db.name
}

func (db *DB) CreateBucket(bucketName string) error {
	return db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("create bucket %s: %w", bucketName, err)
		}
		return nil
	})
}

func (db *DB) DeleteBucket(bucketName string) error {
	return db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("delete bucket %s: %w", bucketName, err)
		}
		return nil
	})
}

func (db *DB) ListBuckets() ([]string, error) {
	var buckets []string
	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			buckets = append(buckets, string(name))
			return nil
		})
	})
	return buckets, err
}

func (db *DB) Put(bucketName string, key string, value interface{}) error {
	if key == "" {
		return err.New("key cannot be empty")
	}
	if value == nil {
		return errors.ErrNilValue
	}

	data, err := js.Marshal(value)
	if err != nil {
		return fmt.Errorf("error marshaling data: %w", err)
	}

	compressedData := compression.CompressData(data)

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.ErrBucketMissing
		}
		return b.Put([]byte(key), compressedData)
	})
}

func (db *DB) Get(bucketName string, key string, target interface{}) error {
	if key == "" {
		return err.New("key cannot be empty")
	}
	if target == nil {
		return errors.ErrNilValue
	}

	var needsMigration bool
	var rawData []byte

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.ErrBucketMissing
		}

		data := b.Get([]byte(key))
		if data == nil {
			return errors.ErrNotFound
		}

		if len(data) == 0 {
			return errors.ErrInvalidData
		}

		rawData = make([]byte, len(data))
		copy(rawData, data)

		actualData := compression.DecompressData(data)

		if len(data) > 0 && (data[0] == 0 || data[0] == 1) && len(actualData) > 50 {
			needsMigration = true
		}

		return js.Unmarshal(actualData, target)
	})

	if err != nil {
		return err
	}

	if needsMigration {
		go func() {
			db.Put(bucketName, key, target)
		}()
	}

	return nil
}

func (db *DB) Delete(bucketName string, key string) error {
	if key == "" {
		return err.New("key cannot be empty")
	}

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.ErrBucketMissing
		}
		return b.Delete([]byte(key))
	})
}

func (db *DB) List(bucketName string) ([]string, error) {
	var keys []string

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.ErrBucketMissing
		}

		return b.ForEach(func(k, _ []byte) error {
			keys = append(keys, string(k))
			return nil
		})
	})

	return keys, err
}

func (db *DB) ForEach(bucketName string, fn func(k, v []byte) error) error {
	return db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.ErrBucketMissing
		}
		return b.ForEach(func(k, v []byte) error {
			actualData := compression.DecompressData(v)
			return fn(k, actualData)
		})
	})
}

func (db *DB) Count(bucketName string) (int, error) {
	var count int
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.ErrBucketMissing
		}

		count = b.Stats().KeyN
		return nil
	})
	return count, err
}

func (db *DB) Batch(fn func(tx *bolt.Tx) error) error {
	return db.Update(fn)
}

func (db *DB) GetAll(bucketName string, constructor func() interface{}) ([]interface{}, error) {
	count, _ := db.Count(bucketName)
	items := make([]interface{}, 0, count)

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.ErrBucketMissing
		}

		return b.ForEach(func(_, v []byte) error {
			if v == nil {
				return nil
			}

			actualData := compression.DecompressData(v)

			item := constructor()
			if err := js.Unmarshal(actualData, item); err != nil {
				return nil
			}
			items = append(items, item)
			return nil
		})
	})

	return items, err
}

func (db *DB) GetAllTyped(bucketName string, itemType reflect.Type) (interface{}, error) {
	sliceType := reflect.SliceOf(itemType)
	result := reflect.MakeSlice(sliceType, 0, 10)

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.ErrBucketMissing
		}

		return b.ForEach(func(_, v []byte) error {
			if v == nil {
				return nil
			}

			actualData := compression.DecompressData(v)

			item := reflect.New(itemType).Interface()
			if err := js.Unmarshal(actualData, item); err != nil {
				return nil
			}

			elem := reflect.ValueOf(item).Elem()
			result = reflect.Append(result, elem)
			return nil
		})
	})

	return result.Interface(), err
}

func (db *DB) Clear(bucketName string) error {
	return db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte(bucketName)); err != nil {
			return fmt.Errorf("delete bucket: %w", err)
		}
		if _, err := tx.CreateBucket([]byte(bucketName)); err != nil {
			return fmt.Errorf("recreate bucket: %w", err)
		}
		return nil
	})
}

func (db *DB) Backup(filename string) error {
	return db.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(filename, 0600)
	})
}

func (db *DB) Stats() bolt.Stats {
	return db.DB.Stats()
}

func (db *DB) Transaction(writable bool, fn func(tx *bolt.Tx) error) error {
	if writable {
		return db.Update(fn)
	}
	return db.View(fn)
}

func (db *DB) Health() error {
	return db.View(func(tx *bolt.Tx) error {
		return nil
	})
}

func (db *DB) GetDiskUsage() (int64, error) {
	info, err := os.Stat(db.DB.Path())
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (db *DB) RefreshBuckets() error {
	return reflection.FindAndInitBuckets(db.DB, db.name)
}

func (db *DB) CompressBucket(bucketName string) error {
	if bucketName == "" {
		return err.New("bucket name cannot be empty")
	}

	var processed int
	var compressionErrors []string

	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return errors.ErrBucketMissing
		}

		return bucket.ForEach(func(k, v []byte) error {
			if len(v) == 0 {
				return nil
			}

			decompressed := compression.DecompressData(v)
			recompressed := compression.CompressData(decompressed)

			if len(recompressed) < len(v) {
				if err := bucket.Put(k, recompressed); err != nil {
					compressionErrors = append(compressionErrors, fmt.Sprintf("key '%s': %v", string(k), err))
					return nil
				}
			}

			processed++
			return nil
		})
	})

	if err != nil {
		return fmt.Errorf("failed to compress bucket '%s': %w", bucketName, err)
	}

	if len(compressionErrors) > 0 {
		return fmt.Errorf("compression completed with %d errors: %s", len(compressionErrors), strings.Join(compressionErrors, "; "))
	}

	logger.Success("Compressed bucket '%s': %d records processed", bucketName, processed)
	return nil
}
