package database

import (
	"fmt"
	"strings"
	"sync"

	"github.com/andr1ww/odin/errors"
	"github.com/andr1ww/odin/internal/logger"
)

type DatabaseManager struct {
	databases map[string]*DB
	mutex     sync.RWMutex
	defaultDB string
}

var (
	manager *DatabaseManager
	once    sync.Once
)

func init() {
	once.Do(func() {
		manager = &DatabaseManager{
			databases: make(map[string]*DB),
		}
	})
}

func Connect(name, dbPath string) error {
	if name == "" {
		name = "main"
	}
	if dbPath == "" {
		dbPath = fmt.Sprintf("%s.db", name)
	}

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if _, exists := manager.databases[name]; exists {
		return errors.ErrDatabaseExists
	}

	db, err := openDatabase(name, dbPath)
	if err != nil {
		return err
	}

	manager.databases[name] = db

	if manager.defaultDB == "" {
		manager.defaultDB = name
	}

	logger.Success("database '%s' connected successfully at %s", name, dbPath)
	return nil
}

func ConnectDefault(dbPath string) error {
	return Connect("main", dbPath)
}

func SetDefault(name string) error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if _, exists := manager.databases[name]; !exists {
		return errors.ErrDatabaseNotFound
	}

	manager.defaultDB = name
	logger.Success("default database set to '%s'", name)
	return nil
}

func Get() (*DB, error) {
	return GetNamed("")
}

func GetNamed(name string) (*DB, error) {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if name == "" {
		name = manager.defaultDB
		if name == "" {
			return nil, errors.ErrNoDefaultDatabase
		}
	}

	db, exists := manager.databases[name]
	if !exists {
		return nil, fmt.Errorf("database '%s' not found", name)
	}

	return db, nil
}

func GetAll() map[string]*DB {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	result := make(map[string]*DB)
	for name, db := range manager.databases {
		result[name] = db
	}
	return result
}

func ListDatabases() []string {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	names := make([]string, 0, len(manager.databases))
	for name := range manager.databases {
		names = append(names, name)
	}
	return names
}

func Close(name string) error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if name == "" {
		name = manager.defaultDB
		if name == "" {
			return errors.ErrNoDefaultDatabase
		}
	}

	db, exists := manager.databases[name]
	if !exists {
		return fmt.Errorf("database '%s' not found", name)
	}

	err := db.DB.Close()
	if err != nil {
		return fmt.Errorf("error closing database '%s': %w", name, err)
	}

	delete(manager.databases, name)

	if manager.defaultDB == name {
		manager.defaultDB = ""
		for dbName := range manager.databases {
			manager.defaultDB = dbName
			break
		}
	}

	logger.Success("Database '%s' connection closed successfully", name)
	return nil
}

func CloseAll() error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	var errors []string
	for name, db := range manager.databases {
		if err := db.DB.Close(); err != nil {
			errors = append(errors, fmt.Sprintf("error closing database '%s': %v", name, err))
		}
	}

	manager.databases = make(map[string]*DB)
	manager.defaultDB = ""

	if len(errors) > 0 {
		return fmt.Errorf("errors closing databases: %s", strings.Join(errors, "; "))
	}

	logger.Success("database connections closed successfully")
	return nil
}
