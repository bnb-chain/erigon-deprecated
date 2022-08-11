// Hook go-metrics into expvar
// on any /debug/metrics request, load all vars from the registry into expvar, and execute regular expvar handler
package exp

import (
	"fmt"
	"net/http"

	metrics2 "github.com/VictoriaMetrics/metrics"
	ethmetrics "github.com/ethereum/go-ethereum/metrics"
	ethprometheus "github.com/ethereum/go-ethereum/metrics/prometheus"
	"github.com/gorilla/mux"
	"github.com/ledgerwatch/log/v3"
)

// Setup starts a dedicated metrics server at the given address.
// This function enables metrics reporting separate from pprof.
func Setup(address string) {
	router := mux.NewRouter()
	router.HandleFunc("/debug/metrics/prometheus", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		metrics2.WritePrometheus(w, true)
	})
	router.Handle("/debug/metrics/prometheus2", ethprometheus.Handler(ethmetrics.DefaultRegistry))
	//m.Handle("/debug/metrics", ExpHandler(metrics.DefaultRegistry))
	//m.Handle("/debug/metrics/prometheus2", promhttp.HandlerFor(prometheus2.DefaultGatherer, promhttp.HandlerOpts{
	//	EnableOpenMetrics: true,
	//}))
	log.Info("Starting metrics server", "addr", fmt.Sprintf("http://%s/debug/metrics/prometheus", address))
	go func() {
		if err := http.ListenAndServe(address, router); err != nil {
			log.Error("Failure in running metrics server", "err", err)
		}
	}()
}
