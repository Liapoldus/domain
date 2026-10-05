package main

import (
	"encoding/json"
	"os"

	"github.com/Liapoldus/domain/internal/application"
	"github.com/Liapoldus/domain/internal/domain/models"
)

func main() {
	var request struct {
		Previous models.Model `json:"previous"`
		Next     models.Model `json:"next"`
	}
	if err := json.NewDecoder(os.Stdin).Decode(&request); err != nil {
		os.Exit(2)
	}
	if err := json.NewEncoder(os.Stdout).Encode(application.PlanMigration(request.Previous, request.Next)); err != nil {
		os.Exit(2)
	}
}
