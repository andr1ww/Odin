package bucket

import (
	"errors"
	"time"

	"github.com/andr1ww/odin/database"
	"github.com/andr1ww/odin/internal/indexing"
	"github.com/andr1ww/odin/internal/reflection"
)

type Bucket struct {
	ID        string     `json:"id"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
	dbName    string
}

var BucketModels = make(map[string]func() interface{})

func (b *Bucket) BeforeSave() {
	now := time.Now()
	if b.CreatedAt.IsZero() {
		b.CreatedAt = now
	}
	b.UpdatedAt = now
}

func (b *Bucket) SetDatabase(dbName string) {
	b.dbName = dbName
}

func (b *Bucket) GetDatabase() string {
	return b.dbName
}

func RegisterBucketModel(model interface{}, constructor func() interface{}) error {
	bucketName, err := reflection.GetBucketName(model)
	if err != nil {
		return err
	}

	BucketModels[bucketName] = constructor
	return nil
}

func (b *Bucket) Save(entity interface{}) error {
	dbName, err := reflection.GetBucketDatabase(entity)
	if err != nil {
		return err
	}

	return b.SaveToDatabase(dbName, entity)
}

func (b *Bucket) SaveToDatabase(dbName string, entity interface{}) error {
	db, err := database.GetNamed(dbName)
	if err != nil {
		return err
	}

	b.BeforeSave()

	bucketName, err := reflection.GetBucketName(entity)
	if err != nil {
		return err
	}

	id := b.ID
	if id == "" {
		return errors.New("ID field is required")
	}

	indexing.UpdateIndex(bucketName, id, entity)
	return db.Put(bucketName, id, entity)
}

func (b *Bucket) Delete(entity interface{}) error {
	dbName, err := reflection.GetBucketDatabase(entity)
	if err != nil {
		return err
	}

	return b.DeleteFromDatabase(dbName, entity)
}

func (b *Bucket) DeleteFromDatabase(dbName string, entity interface{}) error {
	db, err := database.GetNamed(dbName)
	if err != nil {
		return err
	}

	bucketName, err := reflection.GetBucketName(entity)
	if err != nil {
		return err
	}

	id := b.ID
	if id == "" {
		return errors.New("ID field is required")
	}

	indexing.RemoveFromIndex(bucketName, id, entity)
	return db.Delete(bucketName, id)
}

func (b *Bucket) SoftDelete(entity interface{}) error {
	dbName, err := reflection.GetBucketDatabase(entity)
	if err != nil {
		return err
	}

	return b.SoftDeleteFromDatabase(dbName, entity)
}

func (b *Bucket) SoftDeleteFromDatabase(dbName string, entity interface{}) error {
	now := time.Now()
	b.DeletedAt = &now
	return b.SaveToDatabase(dbName, entity)
}
