package main

import (
	"database/sql"
	"github.com/gorilla/feeds"
	"github.com/gorilla/mux"
	"github.com/lib/pq"
	"gopkg.in/yaml.v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type News struct {
	Id            int
	Title         string
	URL           string
	Description   string
	PublishedDate string
	CrawlDate     string
	Source        string
	Tickers       []string
	Tags          []string
}

type Database struct {
	Database string `yaml:"Database"`
	User     string `yaml:"User"`
	Password string `yaml:"Password"`
	Host     string `yaml:"Host"`
	Port     string `yaml:"Port"`
}

func (d *Database) getStr() string {
	return "host=" + d.Host + " port=" + d.Port + " user=" + d.User + " password=" + d.Password + " dbname=" + d.Database + " sslmode=disable"
}

type Config struct {
	Database Database `yaml:"Database"`
	Port     string   `yaml:"Port"`
	Host     string   `yaml:"Host"`
	Domain   string   `yaml:"Domain"`
	Key      string   `yaml:"Key"`
}

type App struct {
	Router *mux.Router
	DB     *sql.DB
	Config *Config
}

func (a *App) Init(config *Config) {
	a.Router = mux.NewRouter()
	a.Config = config

	s := config.Database.getStr()
	var err error
	a.DB, err = sql.Open("postgres", s)
	if err != nil {
		panic(err)
	}

	a.Router.HandleFunc("/rss/symbol/{symbol}", a.getNews)
	a.Router.HandleFunc("/rss/tag/{tag}", a.getTags)
	a.Router.HandleFunc("/rss/stock/news", a.getStockNews)
}

func (a *App) getTags(w http.ResponseWriter, r *http.Request) {

	token := r.URL.Query().Get("token")
	if token != a.Config.Key {
		w.Write([]byte("eror"))
		return
	}

	vars := mux.Vars(r)

	tag := vars["tag"]

	now := time.Now()
	var feed feeds.Feed
	feed.Title = tag + " News"
	feed.Link = &feeds.Link{Href: a.Config.Domain}
	feed.Description = tag + " News"
	feed.Author = &feeds.Author{Name: "Jason Trump"}
	feed.Created = now
	var items []*feeds.Item
	//feed.Items=items

	sqlStmt := "SELECT id,title,url,description,publishedDate,crawlDate,source,tickers,tags FROM NEWS where $1 = ANY(tags) ORDER BY id DESC LIMIT $2"

	var l []News
	_ = l
	rows, err := a.DB.Query(sqlStmt, tag, 100)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var n News
		n = News{}
		err = rows.Scan(&n.Id, &n.Title, &n.URL, &n.Description, &n.PublishedDate, &n.CrawlDate, &n.Source, pq.Array(&n.Tickers), pq.Array(&n.Tags))
		//fmt.Println(n)
		if err != nil {
			panic(err)
		}
		l = append(l, n)

		var x feeds.Item
		x.Title = n.Title
		x.Link = &feeds.Link{Href: n.URL}
		x.Description = n.Description
		x.Source = &feeds.Link{Href: n.Source}
		tm, err := time.Parse(time.RFC3339, n.PublishedDate)
		if err != nil {
			panic(err)
		}
		x.Id = strconv.Itoa(n.Id)
		x.Created = tm

		items = append(items, &x)
	}

	feed.Items = items

	st, err := feed.ToRss()
	if err != nil {
		panic(err)
	}

	w.Write([]byte(st))
}

func (a *App) getStockNews(w http.ResponseWriter, r *http.Request) {

	token := r.URL.Query().Get("token")
	if token != a.Config.Key {
		w.Write([]byte("eror"))
		return
	}

	now := time.Now()
	var feed feeds.Feed
	feed.Title = "Stock" + " News"
	feed.Link = &feeds.Link{Href: a.Config.Domain}
	feed.Description = "Stock" + " News"
	feed.Author = &feeds.Author{Name: "Jason Trump"}
	feed.Created = now
	var items []*feeds.Item
	//feed.Items=items

	sqlStmt := "SELECT id,title,url,description,publishedDate,crawlDate,source,tickers,tags FROM NEWS WHERE cardinality(tickers)>0 ORDER BY id DESC LIMIT $1"
	var l []News
	_ = l
	rows, err := a.DB.Query(sqlStmt, 1000)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var n News
		n = News{}
		err = rows.Scan(&n.Id, &n.Title, &n.URL, &n.Description, &n.PublishedDate, &n.CrawlDate, &n.Source, pq.Array(&n.Tickers), pq.Array(&n.Tags))
		//fmt.Println(n)
		if err != nil {
			panic(err)
		}
		l = append(l, n)

		var x feeds.Item
		x.Title = n.Title
		x.Link = &feeds.Link{Href: n.URL}
		x.Description = n.Description
		x.Source = &feeds.Link{Href: n.Source}
		tm, err := time.Parse(time.RFC3339, n.PublishedDate)
		if err != nil {
			panic(err)
		}
		x.Id = strconv.Itoa(n.Id)
		x.Created = tm

		items = append(items, &x)
	}

	feed.Items = items

	st, err := feed.ToRss()
	if err != nil {
		panic(err)
	}

	w.Write([]byte(st))
}

func (a *App) getNews(w http.ResponseWriter, r *http.Request) {

	token := r.URL.Query().Get("token")
	if token != a.Config.Key {
		w.Write([]byte("eror"))
		return
	}

	vars := mux.Vars(r)

	symbol := vars["symbol"]

	now := time.Now()
	var feed feeds.Feed
	feed.Title = strings.ToUpper(symbol) + " News"
	feed.Link = &feeds.Link{Href: a.Config.Domain}
	feed.Description = strings.ToUpper(symbol) + " News"
	feed.Author = &feeds.Author{Name: "Jason Trump"}
	feed.Created = now
	var items []*feeds.Item
	//feed.Items=items

	sqlStmt := "SELECT id,title,url,description,publishedDate,crawlDate,source,tickers,tags FROM NEWS where $1 = ANY(tickers) ORDER BY id DESC LIMIT $2"
	var l []News
	_ = l
	rows, err := a.DB.Query(sqlStmt, strings.ToLower(symbol), 100)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var n News
		n = News{}
		err = rows.Scan(&n.Id, &n.Title, &n.URL, &n.Description, &n.PublishedDate, &n.CrawlDate, &n.Source, pq.Array(&n.Tickers), pq.Array(&n.Tags))
		//fmt.Println(n)
		if err != nil {
			panic(err)
		}
		l = append(l, n)

		var x feeds.Item
		x.Title = n.Title
		x.Link = &feeds.Link{Href: n.URL}
		x.Description = n.Description
		x.Source = &feeds.Link{Href: n.Source}
		tm, err := time.Parse(time.RFC3339, n.PublishedDate)
		if err != nil {
			panic(err)
		}
		x.Id = strconv.Itoa(n.Id)
		x.Created = tm

		items = append(items, &x)
	}

	feed.Items = items

	st, err := feed.ToRss()
	if err != nil {
		panic(err)
	}

	w.Write([]byte(st))
}

func main() {
	dat, err := os.ReadFile("config.yaml")
	if err != nil {
		panic(err)
	}

	cnf := Config{}
	err = yaml.Unmarshal([]byte(dat), &cnf)
	if err != nil {
		panic(err)
	}

	var a App
	a.Init(&cnf)

	http.ListenAndServe(cnf.Host+":"+cnf.Port, a.Router)

}
