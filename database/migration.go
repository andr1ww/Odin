package database

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/andr1ww/odin/errors"
	"github.com/andr1ww/odin/internal/compression"
	"github.com/andr1ww/odin/internal/logger"
	bolt "go.etcd.io/bbolt"
)

func (db *DB) MigrateBucket(bucketName, targetDBName string, deleteSource bool) error {
	if bucketName == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}
	if targetDBName == "" {
		return fmt.Errorf("target database name cannot be empty")
	}
	if targetDBName == db.name {
		return fmt.Errorf("source and target database cannot be the same")
	}

	targetDB, err := GetNamed(targetDBName)
	if err != nil {
		return fmt.Errorf("failed to get target database '%s': %w", targetDBName, err)
	}

	if err := targetDB.CreateBucket(bucketName); err != nil {
		return fmt.Errorf("failed to create bucket in target database: %w", err)
	}

	var migrationCount int
	var migrationErrors []string

	err = db.View(func(sourceTx *bolt.Tx) error {
		sourceBucket := sourceTx.Bucket([]byte(bucketName))
		if sourceBucket == nil {
			return fmt.Errorf("bucket '%s' not found in source database", bucketName)
		}

		return sourceBucket.ForEach(func(k, v []byte) error {
			actualData := compression.DecompressData(v)

			err := targetDB.Update(func(targetTx *bolt.Tx) error {
				targetBucket := targetTx.Bucket([]byte(bucketName))
				if targetBucket == nil {
					return fmt.Errorf("bucket '%s' not found in target database", bucketName)
				}

				compressedData := compression.CompressData(actualData)
				return targetBucket.Put(k, compressedData)
			})

			if err != nil {
				migrationErrors = append(migrationErrors, fmt.Sprintf("key %s: %v", string(k), err))
				return nil
			}

			migrationCount++
			return nil
		})
	})

	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	if len(migrationErrors) > 0 {
		return fmt.Errorf("migration completed with %d errors: %s", len(migrationErrors), strings.Join(migrationErrors, "; "))
	}

	if deleteSource {
		if err := db.DeleteBucket(bucketName); err != nil {
			return fmt.Errorf("failed to delete source bucket after successful migration: %w", err)
		}
	}

	logger.Success("Migrated bucket '%s' from database '%s' to '%s' (%d records)", bucketName, db.name, targetDBName, migrationCount)
	return nil
}

func (db *DB) MigrateBucketWithTransform(bucketName, targetDBName string, transform func(key []byte, data []byte) ([]byte, []byte, error), deleteSource bool) error {
	if bucketName == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}
	if targetDBName == "" {
		return fmt.Errorf("target database name cannot be empty")
	}
	if targetDBName == db.name {
		return fmt.Errorf("source and target database cannot be the same")
	}
	if transform == nil {
		return fmt.Errorf("transform function cannot be nil")
	}

	targetDB, err := GetNamed(targetDBName)
	if err != nil {
		return fmt.Errorf("failed to get target database '%s': %w", targetDBName, err)
	}

	if err := targetDB.CreateBucket(bucketName); err != nil {
		return fmt.Errorf("failed to create bucket in target database: %w", err)
	}

	var migrationCount int
	var migrationErrors []string

	err = db.View(func(sourceTx *bolt.Tx) error {
		sourceBucket := sourceTx.Bucket([]byte(bucketName))
		if sourceBucket == nil {
			return fmt.Errorf("bucket '%s' not found in source database", bucketName)
		}

		return sourceBucket.ForEach(func(k, v []byte) error {
			actualData := compression.DecompressData(v)

			newKey, newData, err := transform(k, actualData)
			if err != nil {
				migrationErrors = append(migrationErrors, fmt.Sprintf("transform key %s: %v", string(k), err))
				return nil
			}

			if newKey == nil || newData == nil {
				return nil
			}

			err = targetDB.Update(func(targetTx *bolt.Tx) error {
				targetBucket := targetTx.Bucket([]byte(bucketName))
				if targetBucket == nil {
					return fmt.Errorf("bucket '%s' not found in target database", bucketName)
				}

				compressedData := compression.CompressData(newData)
				return targetBucket.Put(newKey, compressedData)
			})

			if err != nil {
				migrationErrors = append(migrationErrors, fmt.Sprintf("key %s: %v", string(newKey), err))
				return nil
			}

			migrationCount++
			return nil
		})
	})

	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	if len(migrationErrors) > 0 {
		return fmt.Errorf("migration completed with %d errors: %s", len(migrationErrors), strings.Join(migrationErrors, "; "))
	}

	if deleteSource {
		if err := db.DeleteBucket(bucketName); err != nil {
			return fmt.Errorf("failed to delete source bucket after successful migration: %w", err)
		}
	}

	logger.Success("Migrated bucket '%s' from database '%s' to '%s' with transform (%d records)", bucketName, db.name, targetDBName, migrationCount)
	return nil
}

func MigrateBucketBetweenDatabases(sourceBucketName, sourceDBName, targetBucketName, targetDBName string, deleteSource bool) error {
	if sourceBucketName == "" {
		return fmt.Errorf("source bucket name cannot be empty")
	}
	if sourceDBName == "" {
		return fmt.Errorf("source database name cannot be empty")
	}
	if targetBucketName == "" {
		targetBucketName = sourceBucketName
	}
	if targetDBName == "" {
		return fmt.Errorf("target database name cannot be empty")
	}

	sourceDB, err := GetNamed(sourceDBName)
	if err != nil {
		return fmt.Errorf("failed to get source database '%s': %w", sourceDBName, err)
	}

	targetDB, err := GetNamed(targetDBName)
	if err != nil {
		return fmt.Errorf("failed to get target database '%s': %w", targetDBName, err)
	}

	if err := targetDB.CreateBucket(targetBucketName); err != nil {
		return fmt.Errorf("failed to create bucket in target database: %w", err)
	}

	var migrationCount int
	var migrationErrors []string

	err = sourceDB.View(func(sourceTx *bolt.Tx) error {
		sourceBucket := sourceTx.Bucket([]byte(sourceBucketName))
		if sourceBucket == nil {
			return fmt.Errorf("bucket '%s' not found in source database", sourceBucketName)
		}

		return sourceBucket.ForEach(func(k, v []byte) error {
			actualData := compression.DecompressData(v)

			err := targetDB.Update(func(targetTx *bolt.Tx) error {
				targetBucket := targetTx.Bucket([]byte(targetBucketName))
				if targetBucket == nil {
					return fmt.Errorf("bucket '%s' not found in target database", targetBucketName)
				}

				compressedData := compression.CompressData(actualData)
				return targetBucket.Put(k, compressedData)
			})

			if err != nil {
				migrationErrors = append(migrationErrors, fmt.Sprintf("key %s: %v", string(k), err))
				return nil
			}

			migrationCount++
			return nil
		})
	})

	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	if len(migrationErrors) > 0 {
		return fmt.Errorf("migration completed with %d errors: %s", len(migrationErrors), strings.Join(migrationErrors, "; "))
	}

	if deleteSource {
		if err := sourceDB.DeleteBucket(sourceBucketName); err != nil {
			return fmt.Errorf("failed to delete source bucket after successful migration: %w", err)
		}
	}

	logger.Success("Migrated bucket '%s' from database '%s' to bucket '%s' in database '%s' (%d records)", sourceBucketName, sourceDBName, targetBucketName, targetDBName, migrationCount)
	return nil
}

func (db *DB) Compact() error {
	tempPath := db.name + "_temp.db"

	tempDB, err := bolt.Open(tempPath, 0600, &bolt.Options{
		Timeout:         10 * time.Second,
		InitialMmapSize: 10 * 1024 * 1024,
		PageSize:        8096,
		NoSync:          false,
		NoFreelistSync:  false,
		FreelistType:    bolt.FreelistMapType,
		NoGrowSync:      true,
		MmapFlags:       0,
	})
	if err != nil {
		return fmt.Errorf("failed to create temp database: %w", err)
	}

	err = db.View(func(sourceTx *bolt.Tx) error {
		return tempDB.Update(func(targetTx *bolt.Tx) error {
			return sourceTx.ForEach(func(bucketName []byte, sourceBucket *bolt.Bucket) error {
				targetBucket, err := targetTx.CreateBucket(bucketName)
				if err != nil {
					return fmt.Errorf("failed to create bucket %s: %w", string(bucketName), err)
				}

				return sourceBucket.ForEach(func(k, v []byte) error {
					return targetBucket.Put(k, v)
				})
			})
		})
	})

	if err != nil {
		tempDB.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to copy data: %w", err)
	}

	tempDB.Close()

	originalPath := db.DB.Path()
	backupPath := originalPath + ".backup"

	if err := db.DB.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to close original database: %w", err)
	}

	if err := os.Rename(originalPath, backupPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to backup original database: %w", err)
	}

	if err := os.Rename(tempPath, originalPath); err != nil {
		os.Rename(backupPath, originalPath)
		return fmt.Errorf("failed to replace database: %w", err)
	}

	newDB, err := bolt.Open(originalPath, 0600, &bolt.Options{
		Timeout:         10 * time.Second,
		InitialMmapSize: 10 * 1024 * 1024,
		PageSize:        8096,
		NoSync:          false,
		NoFreelistSync:  true,
		FreelistType:    bolt.FreelistMapType,
		NoGrowSync:      true,
		MmapFlags:       0,
	})
	if err != nil {
		os.Rename(backupPath, originalPath)
		return fmt.Errorf("failed to reopen database: %w", err)
	}

	db.DB = newDB
	os.Remove(backupPath)

	logger.Success("Database '%s' compacted successfully", db.name)
	return nil
}

func (db *DB) CompactBucket(bucketName string) error {
	return db.Update(func(tx *bolt.Tx) error {
		sourceBucket := tx.Bucket([]byte(bucketName))
		if sourceBucket == nil {
			return errors.ErrBucketMissing
		}

		tempBucketName := bucketName + "_temp"
		tempBucket, err := tx.CreateBucket([]byte(tempBucketName))
		if err != nil {
			return fmt.Errorf("failed to create temp bucket: %w", err)
		}

		err = sourceBucket.ForEach(func(k, v []byte) error {
			return tempBucket.Put(k, v)
		})
		if err != nil {
			tx.DeleteBucket([]byte(tempBucketName))
			return fmt.Errorf("failed to copy data: %w", err)
		}

		if err := tx.DeleteBucket([]byte(bucketName)); err != nil {
			return fmt.Errorf("failed to delete original bucket: %w", err)
		}

		newBucket, err := tx.CreateBucket([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("failed to recreate bucket: %w", err)
		}

		err = tempBucket.ForEach(func(k, v []byte) error {
			return newBucket.Put(k, v)
		})
		if err != nil {
			return fmt.Errorf("failed to restore data: %w", err)
		}

		return tx.DeleteBucket([]byte(tempBucketName))
	})
}

func (db *DB) CompressAllBuckets() error {
	buckets, err := db.ListBuckets()
	if err != nil {
		return fmt.Errorf("failed to list buckets: %w", err)
	}

	if len(buckets) == 0 {
		logger.Warning("No buckets found in database '%s'", db.name)
		return nil
	}

	var totalProcessed int
	var totalErrors []string

	logger.Success("Starting compression for %d buckets in database '%s'", len(buckets), db.name)

	for _, bucketName := range buckets {
		bucketProcessed := 0
		bucketErrors := 0

		err := db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(bucketName))
			if bucket == nil {
				return fmt.Errorf("bucket '%s' not found", bucketName)
			}

			return bucket.ForEach(func(k, v []byte) error {
				if len(v) == 0 {
					return nil
				}

				decompressed := compression.DecompressData(v)
				recompressed := compression.CompressData(decompressed)

				if len(recompressed) < len(v) {
					if err := bucket.Put(k, recompressed); err != nil {
						bucketErrors++
						totalErrors = append(totalErrors, fmt.Sprintf("bucket '%s', key '%s': %v", bucketName, string(k), err))
						return nil
					}
				}

				bucketProcessed++
				totalProcessed++
				return nil
			})
		})

		if err != nil {
			totalErrors = append(totalErrors, fmt.Sprintf("bucket '%s': %v", bucketName, err))
		}

		logger.Success("Compressed bucket '%s': %d records processed, %d errors", bucketName, bucketProcessed, bucketErrors)
	}

	if len(totalErrors) > 0 {
		logger.Error("Compression completed with %d total errors", len(totalErrors))
		for _, errMsg := range totalErrors {
			logger.Error("  %s", errMsg)
		}
		return fmt.Errorf("compression completed with %d errors", len(totalErrors))
	}

	logger.Success("Successfully compressed all buckets in database '%s': %d total records processed", db.name, totalProcessed)
	return nil
}
