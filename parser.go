package main

import (
    "os"
    "io"
    "fmt"
    "flag"
    "time"
    "io/ioutil"
    "database/sql"
    "encoding/xml"
    "path/filepath"
    "golang.org/x/net/html/charset"
)

import _ "gitlab.eltex.loc/aleksandr.rvachev/go-sqlite3.git"

type Programm struct {
 Start                 string             `xml:"start,attr"`
 Channel               string             `xml:"channel,attr"`
 Title                 string             `xml:"title"`
 Description           string             `xml:"desc"`
}

var db *sql.DB
var sql1, sql2 *sql.Stmt

func main() {
  dbPath := flag.String("input", "output.slite", "database file")
  xmlPath := flag.String("output", "week.xml", "XMLTV file")
  flag.Parse()

  outDir := filepath.Dir(*dbPath)

  tmpFile, tmpErr := ioutil.TempFile(outDir, "db-")
  if tmpErr != nil {
    panic("Cannot create temporary file" + tmpErr.Error())
  }

  dbUrl := fmt.Sprintf("file:%s", tmpFile.Name())

  db, dbErr := sql.Open("sqlite3", dbUrl)
  if dbErr != nil {
    panic("error: " + dbErr.Error())
  }

  defer db.Close()

  db.Exec("CREATE TABLE search_meta (docid INTEGER PRIMARY KEY, ch_id, start_time, description);")
  db.Exec("CREATE VIRTUAL TABLE fts_search USING fts4(content='search_meta', channel_name, description);")

  xmlFile, inputErr := os.Open(*xmlPath)
  if inputErr != nil {
    panic("Could not open file: week.xml")
  }

  decoder := xml.NewDecoder(xmlFile)
  decoder.CharsetReader = charset.NewReaderLabel

  programme := &Programm{}

  bulkTx, txErr := db.Begin()
  if txErr != nil {
    panic("Could not start transaction" + txErr.Error())
  }

  sql1, _ = bulkTx.Prepare("INSERT INTO search_meta (start_time, ch_id, description) VALUES (?, ?, ?);")
  sql2, _ = bulkTx.Prepare("INSERT INTO fts_search (docid, description) VALUES (?, ?);")

  // skip root
root:
  for {
    token, xmlErr := decoder.Token()
    if xmlErr != nil {
      panic("XMLTV file is malformed (failed to find root tag)")
    }

    switch xmlRoot := token.(type) {
      default:
        continue;
      case xml.StartElement:
        if (xmlRoot.Name.Local == "tv") {
          break root;
        } else {
          panic(fmt.Sprintf("malformed XMLTV: <tv> tag not found, got <%s> instead", xmlRoot.Name.Local))
        }
    }
  }

  // iterate over all <programme> tags and add them to database
  for {
    t, tokenErr := decoder.Token()
    if tokenErr != nil {
      if tokenErr == io.EOF {
        break
      } else {
        panic("Failed to read token:" + tokenErr.Error())
      }
    }

    switch startElement := t.(type) {
      default:
        continue;
      case xml.StartElement:
        //fmt.Printf("Another element: %s", startElement.Name.Local)

        if (startElement.Name.Local != "programme") {
          decoder.Skip()
          continue;
        }
        addElement(decoder, programme, &startElement)
        break;
    }
  }

  bulkTx.Commit()

  _, optimizeErr := db.Exec("INSERT INTO fts_search(fts_search) VALUES('optimize');")
  if optimizeErr != nil {
    panic("optimize() failed: " + optimizeErr.Error())
  }
}

func addElement(decoder *xml.Decoder, programme *Programm, xmlElement *xml.StartElement) {
  decErr := decoder.DecodeElement(programme, xmlElement)
  if (decErr != nil) {
    panic("Could not decode element" + decErr.Error())
  }

  //fmt.Printf("Another slot on channel %s...\n", programme.Channel)

  startTime, timeErr := time.ParseInLocation("20060102150405 -0700", programme.Start, time.FixedZone("None", 0))
  if (timeErr != nil) {
    panic("Failed to parse start time: " + timeErr.Error())
  }

  result, metaErr := sql1.Exec(startTime.Unix(), programme.Channel, programme.Description)
  if (metaErr != nil) {
    panic("Meta INSERT failed" + metaErr.Error())
  }

  insertId, _ := result.LastInsertId()

  _, ftsErr := sql2.Exec(insertId, programme.Description)
  if (ftsErr != nil) {
    panic("FTS INSERT failed" + ftsErr.Error())
  }
}
