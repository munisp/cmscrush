package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/munisp/cmscrush/services/decision-service/internal/decision"
	"github.com/munisp/cmscrush/services/decision-service/internal/httpapi"
	"github.com/munisp/cmscrush/services/decision-service/internal/store"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	evaluator := decision.NewEvaluator()
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           httpapi.New(evaluator, store.NewMemoryRepository()).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("decision-service listening on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
