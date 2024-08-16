package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/google/uuid"
	"github.com/thedatashed/xlsxreader"
)

type brand struct {
	id    string
	title string
}

type model struct {
	id      string
	brandID string
	title   string
}

type protocol struct {
	id    string
	title string
}

type port struct {
	id         string
	protocolID string
	value      int
}

type equip struct {
	id          string
	modelID     string
	portID      string
	isPublished bool
}

func main() {
	// Create an instance of the reader by opening a target file
	xl, _ := xlsxreader.OpenFile("./ggg.xlsx")

	// Ensure the file reader is closed once utilised
	defer xl.Close()

	l := log.Default()

	brands := make(map[string]brand)
	models := make(map[string]model)
	protocols := make(map[string]protocol)
	ports := make(map[string]port)

	equips := make([]equip, 0)

	// Iterate on the rows of data
	for row := range xl.ReadRows(xl.Sheets[0]) {
		if row.Index == 0 {
			continue
		}

		if len(row.Cells) == 1 {
			break
		}

		if _, ok := brands[row.Cells[3].Value]; !ok {
			brands[row.Cells[3].Value] = brand{
				id:    uuid.New().String(),
				title: row.Cells[3].Value,
			}
		}

		if _, ok := models[row.Cells[2].Value]; !ok {
			if b, okBrand := brands[row.Cells[3].Value]; okBrand {
				models[row.Cells[2].Value] = model{
					id:      uuid.New().String(),
					brandID: b.id,
					title:   row.Cells[2].Value,
				}
			} else {
				log.Println("can`t find brand for model", row.Cells[2].Value)
				continue
			}
		}

		if _, ok := protocols[row.Cells[1].Value]; !ok {
			protocols[row.Cells[1].Value] = protocol{
				id:    uuid.New().String(),
				title: row.Cells[1].Value,
			}
		}

		if _, ok := ports[row.Cells[0].Value]; !ok {
			val, err := strconv.Atoi(row.Cells[0].Value)
			if err != nil {
				log.Default().Printf("can`t convert %s to int", row.Cells[0].Value)
				val = 0
			}

			if p, okProtocol := protocols[row.Cells[1].Value]; okProtocol {
				ports[row.Cells[0].Value] = port{
					id:         uuid.New().String(),
					protocolID: p.id,
					value:      val,
				}
			} else {
				log.Println("can`t find protocol for port", row.Cells[0].Value)
				continue
			}
		}
	}

	for row := range xl.ReadRows(xl.Sheets[0]) {
		var (
			modelID string
			portID  string
		)

		if row.Index == 0 {
			continue
		}

		if len(row.Cells) == 1 {
			break
		}

		if _, ok := models[row.Cells[2].Value]; !ok {
			l.Printf("can`t find model %s", row.Cells[2].Value)
			continue
		}

		modelID = models[row.Cells[2].Value].id

		if _, ok := ports[row.Cells[0].Value]; !ok {
			l.Printf("can`t find port %s", row.Cells[0].Value)
			continue
		}

		portID = ports[row.Cells[0].Value].id

		equips = append(equips, equip{
			id:          uuid.New().String(),
			modelID:     modelID,
			portID:      portID,
			isPublished: true,
		})
	}

	queryTpl := `
-- +goose Up
-- +goose StatementBegin

%s

%s

%s

%s

%s

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM equipments;
DELETE FROM models;
DELETE FROM ports;
DELETE FROM brands;
DELETE FROM protocols;
-- +goose StatementEnd
`

	insertBrands := buildBrandsQuery(brands)
	insertProtocols := buildProtocolsQuery(protocols)
	insertModels := buildModelsQuery(models)
	insertPorts := buildPortsQuery(ports)
	insertEquips := buildEquipsQuery(equips)

	query := fmt.Sprintf(queryTpl, insertBrands, insertProtocols, insertModels, insertPorts, insertEquips)

	fi, err := os.Create("migration.sql")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err = fi.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	_, err = fi.Write([]byte(query))
	if err != nil {
		log.Fatal(err)
	}
}

func buildBrandsQuery(in map[string]brand) string {
	query := `INSERT INTO brands (id, title, created_at) 
VALUES`

	for i := range in {
		query = fmt.Sprintf(
			"%s \n('%s', '%s', now()),",
			query,
			in[i].id,
			in[i].title,
		)
	}

	query = query[0 : len(query)-1]

	return query + ";"
}

func buildProtocolsQuery(in map[string]protocol) string {
	query := `INSERT INTO protocols (id, title, created_at) 
VALUES`

	for i := range in {
		query = fmt.Sprintf(
			"%s \n('%s', '%s', now()),",
			query,
			in[i].id,
			in[i].title,
		)
	}

	query = query[0 : len(query)-1]

	return query + ";"
}

func buildModelsQuery(in map[string]model) string {
	query := `INSERT INTO models (id, title, brand_id, created_at) 
VALUES`

	for i := range in {
		query = fmt.Sprintf(
			"%s \n('%s', '%s', '%s', now()),",
			query,
			in[i].id,
			in[i].title,
			in[i].brandID,
		)
	}

	query = query[0 : len(query)-1]

	return query + ";"
}

func buildPortsQuery(in map[string]port) string {
	query := `INSERT INTO ports (id, protocol_id, port, created_at) 
VALUES`

	for i := range in {
		query = fmt.Sprintf(
			"%s \n('%s', '%s', '%d', now()),",
			query,
			in[i].id,
			in[i].protocolID,
			in[i].value,
		)
	}

	query = query[0 : len(query)-1]

	return query + ";"
}

func buildEquipsQuery(in []equip) string {
	query := `INSERT INTO equipments (id, model_id, port_id, is_published, created_at) 
VALUES`

	for i := range in {
		query = fmt.Sprintf(
			"%s \n('%s', '%s', '%s', %t, now()),",
			query,
			in[i].id,
			in[i].modelID,
			in[i].portID,
			in[i].isPublished,
		)
	}

	query = query[0 : len(query)-1]

	return query + ";"
}
