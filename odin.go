package odin

import (
	"github.com/andr1ww/odin/bucket"
	"github.com/andr1ww/odin/database"
	"github.com/andr1ww/odin/internal/logger"
)

type Bucket = bucket.Bucket
type DB = database.DB

var (
	Connect        = database.Connect
	ConnectDefault = database.ConnectDefault
	SetDefault     = database.SetDefault
	Get            = database.Get
	GetNamed       = database.GetNamed
	GetAll         = database.GetAll
	ListDatabases  = database.ListDatabases
	Close          = database.Close
	CloseAll       = database.CloseAll

	Find      = bucket.Find
	FindWhere = bucket.FindWhere
	Create    = bucket.Create
	FindAll   = bucket.FindAll

	SetLogger      = logger.SetLogger
	DisableLogging = logger.DisableLogging
)
