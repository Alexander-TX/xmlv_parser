package main

import (
    "os"
    "io"
    "fmt"
    "flag"
    "bytes"
    "bufio"
    "strings"
    "runtime"
    "io/ioutil"
    "database/sql"
    "encoding/binary"
    "path/filepath"
    "compress/gzip"
    "archive/zip"
    "golang.org/x/text/encoding"
    "golang.org/x/net/html/charset"

    _ "golang.org/x/text/encoding/charmap"
)

import _ "gitlab.eltex.loc/aleksandr.rvachev/go-sqlite3.git"

var db *sql.DB
var jtv *zip.Writer

var pdtWriter io.Writer
var ndxBuffer bytes.Buffer
var prevChannelId string
var pdtPosition uint16

var charsetName string
var filenameCharset string
var encoder *encoding.Encoder
var namEncoder *encoding.Encoder

var hoursOffset = 0
var channelCount = 0

var rawFileTime = 0

var exitCode = 0

func Bail(format string, a ...interface{}) {
  exitCode = 1
  fmt.Fprintf(os.Stderr, "%s\n", "Fatal error!!")
  fmt.Fprintf(os.Stderr, format, a...)
  runtime.Goexit()
}

func flushNdx() {
  //fmt.Fprintf(os.Stderr, "Flushing %s\n", prevChannelId)

  ndxFileName := prevChannelId + ".ndx"
  if filenameCharset != "UTF-8" {
    ndxFileName, _ = namEncoder.String(ndxFileName)
  }

  jtvNdxHeader := zip.FileHeader{
    Name: ndxFileName,
    Method: 8,
    NonUTF8: charsetName != "UTF-8",
  }
  ndxWriter, _ := jtv.CreateHeader(&jtvNdxHeader)

  // set total entry count
  ndxBytes := ndxBuffer.Bytes()
  ndxEntryCount := (ndxBuffer.Len() - 2) / 12;
  binary.LittleEndian.PutUint16(ndxBytes[0:], uint16(ndxEntryCount))

  _, ndxErr := ndxBuffer.WriteTo(ndxWriter)
  if ndxErr != nil {
    Bail("Writing JTV ndx file failed: %s\n", ndxErr.Error())
  }
}

func main() {
  defer func() {
    if r := recover(); r != nil {
      panic(r)
    } else {
      os.Exit(exitCode)
    }
  }()

  var Usage = func() {
    fmt.Fprintf(os.Stderr, "Simple EPGX to JTV converter\n\n")
    fmt.Fprintf(os.Stderr, "usage: %s [options]\n", os.Args[0])
    flag.PrintDefaults()
  }

  flag.Usage = Usage

  dbPath := flag.String("input", "", "EPGX file. (default none)")
  jtvPath := flag.String("output", "schedule.jtv.zip", "jtv file.")
  jtvCharset := flag.String("charset", "UTF-8", "jtv title charset.")
  zipCharset := flag.String("zip-charset", "UTF-8", "zip filename charset.")
  offsetFlag := flag.Int("offset-time", 0, "number of hours to add to each date")
  flag.Parse()

  hoursOffset = *offsetFlag

  charsetName = *jtvCharset
  if charsetName != "UTF-8" {
    enc, _ := charset.Lookup(charsetName)
    if enc == nil {
      Bail("Encoder for '%s' encoding can not be loaded\n", charsetName)
    }

    encoder = enc.NewEncoder()
  }

  filenameCharset = *zipCharset
  if filenameCharset != "UTF-8" {
    enc, _ := charset.Lookup(filenameCharset)
    if enc == nil {
      Bail("Encoder for '%s' encoding can not be loaded\n", filenameCharset)
    }

    namEncoder = enc.NewEncoder()
  }

  var dbFile *os.File

  if (*dbPath == "") {
    fmt.Fprintf(os.Stderr, "No -input argument, please specify database path\n\n");
    fmt.Fprintf(os.Stderr, "supported arguments:\n")
    flag.PrintDefaults()
    os.Exit(2)
  } else {
    var inputErr error

    dbFile, inputErr = os.Open(*dbPath)
    if inputErr != nil {
      Bail("Could not open EPGX file\n %s\n", inputErr.Error())
    }
  }

  if strings.HasSuffix(*dbPath, ".gz") {
    fmt.Fprintf(os.Stderr, "Detected .gz extension, writing EPGX database to temporary file...\n")

    gzipReader, gzErr := gzip.NewReader(bufio.NewReader(dbFile))
    if gzErr != nil {
      Bail("Failed to open gzip archive\n %s\n", gzErr.Error())
    }

    var tmpDbErr error

    dbFile, tmpDbErr = ioutil.TempFile("", "db-*.sqlite")
    if tmpDbErr != nil {
      Bail("Cannot create temporary db file\n %s\n", tmpDbErr.Error())
    }

    _, copyErr := io.Copy(dbFile, gzipReader)
    if copyErr != nil {
      Bail("Failed to extract db to temporary file\n %s\n", copyErr.Error())
    }

    defer os.Remove(dbFile.Name())
  }

  outDir := filepath.Dir(*jtvPath)

  dbUrl := fmt.Sprintf("file:%s", dbFile.Name())

  db, dbErr := sql.Open("sqlite3", dbUrl)
  if dbErr != nil {
    Bail("sqlite error\n %s\n", dbErr.Error())
  }

  // do not create on-disk temporary files (we don't want to clean them up)
  db.Exec("PRAGMA journal_mode = MEMORY;")
  db.Exec("PRAGMA temp_store = MEMORY;")

  tmpFile, tmpErr := ioutil.TempFile(outDir, "db-*.zip")
  if tmpErr != nil {
    Bail("Cannot create temporary file\n %s\n", tmpErr.Error())
  }

  defer os.Remove(tmpFile.Name())

  jtv = zip.NewWriter(tmpFile)

  fmt.Printf("Copying schedule to JTV archive\n")

  rows, queryErr := db.Query("SELECT ch_id, start_time, (SELECT text FROM text WHERE docid = title_id) FROM search_meta ORDER BY ch_id, start_time ASC")
  if queryErr != nil {
    Bail("Failed to request EPG rows from database\n %s\n", queryErr.Error())
  }

  for rows.Next() {
    var ch_id string
    var start_time int64
    var title string

    scanErr := rows.Scan(&ch_id, &start_time, &title)
    if scanErr != nil {
      Bail("SQLite error\n %s\n", scanErr.Error())
    }

    addItem(ch_id, start_time, title)
  }

  flushNdx()

  jtvError := jtv.Close()
  if jtvError != nil {
    Bail("Failed to save JTV archive: %s\n", jtvError.Error())
  }

  jtvRenameErr := os.Rename(tmpFile.Name(), *jtvPath)
  if (jtvRenameErr != nil) {
    Bail("Failed to move temporary file to output\n %s\n", jtvRenameErr.Error())
  }

  fmt.Fprintf(os.Stderr, "Populated JTV info for %d channels\n", channelCount)
}

func addItem(chId string, startTimeUnix int64, progTitle string) {
  if chId != prevChannelId {
    channelCount += 1

    if prevChannelId != "" {
      flushNdx()
    }

    // reserve space for 2-byte number of entries
    spacer := make([]byte, 2)
    ndxBuffer.Write(spacer)

    prevChannelId = chId

    pdtFileName := chId + ".pdt"
    if filenameCharset != "UTF-8" {
      pdtFileName, _ = namEncoder.String(pdtFileName)
    }

    var jtvPdtHeader = zip.FileHeader{
      Name: pdtFileName,
      Method: 8,
      NonUTF8: filenameCharset != "UTF-8",
    }
    pdtWriter, _ = jtv.CreateHeader(&jtvPdtHeader)

    sig := []byte("JTV 3.x TV Program Data\x0a\x0a\x0a")
    pdtWriter.Write(sig)

    pdtPosition = uint16(len(sig))
  }

  startTimeUnix += int64(hoursOffset * 60 * 60)

  // Magic!!!!
  var filetime = (uint64(startTimeUnix) + 11644473600) * 10000000

  if (rawFileTime == 0) {
    fmt.Fprintf(os.Stderr, "Schedule starts at %d\n", filetime)

    rawFileTime = 1
  }

  ndxBuf := make([]byte, 12)
  binary.LittleEndian.PutUint64(ndxBuf[0:], 0)
  binary.LittleEndian.PutUint64(ndxBuf[2:], filetime)
  binary.LittleEndian.PutUint16(ndxBuf[10:], pdtPosition)
  ndxBuffer.Write(ndxBuf)

  var pdtLen int
  var pdtBuf []byte

  if charsetName != "UTF-8" {
    progTitle, _ = encoder.String(progTitle)
  }

  pdtLen = len(progTitle) + 2
  pdtBuf = make([]byte, pdtLen)
  binary.LittleEndian.PutUint16(pdtBuf[0:], uint16(len(progTitle)))
  copy(pdtBuf[2:], progTitle)

  pdtPosition += uint16(pdtLen)

  _, pdtErr := pdtWriter.Write(pdtBuf)
  if pdtErr != nil {
    Bail("Writing JTV pdt file failed: %s\n", pdtErr.Error())
  }
}
