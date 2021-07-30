// Package metrics contains a prometheus metrics exporter for opni metrics
// posted via nats.
package metrics

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rancher/opni/apis/demo/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	forecastedCPUUsage = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "opni",
		Name:      "forecasted_cpu_usage",
		Help:      "Forecasted CPU usage",
	}, []string{
		"opni",
		"instance",
	})
	forecastedLowerBound = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "opni",
		Name:      "forecasted_lower_bound",
		Help:      "Forecasted lower bound for individual metrics",
	}, []string{
		"opni",
		"metric_name",
	})
	forecastedUpperBound = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "opni",
		Name:      "forecasted_upper_bound",
		Help:      "Forecasted upper bound for individual metrics",
	}, []string{
		"opni",
		"metric_name",
	})
	cpuBacklog = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "opni",
		Name:      "CPU Inference Backlog",
		Help:      "CPU Inference Backlog",
	}, []string{
		"opni",
	})
	gpuBacklog = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "opni",
		Name:      "GPU Inference Backlog",
		Help:      "GPU Inference Backlog",
	}, []string{
		"opni",
	})
)

func init() {
	metrics.Registry.MustRegister(forecastedCPUUsage)
	metrics.Registry.MustRegister(forecastedLowerBound)
	metrics.Registry.MustRegister(forecastedUpperBound)
	metrics.Registry.MustRegister(cpuBacklog)
	metrics.Registry.MustRegister(gpuBacklog)
}

type ForecastedCPUUsage struct {
	Instance string  `json:"instance"`
	Value    float64 `json:"value"`
}

type ForecastedMetricBounds struct {
	YhatUpper  float64 `json:"yhat_upper"`
	YhatLower  float64 `json:"yhat_lower"`
	MetricName string  `json:"metric_name"`
}

type Backlog struct {
	BacklogCount float64 `json:"backlog_count"`
}

func RunMetricsExporter(
	ctx context.Context,
	logger logr.Logger,
	demo *v1alpha1.OpniDemo,
) {
	url := fmt.Sprintf("nats://nats_client:%s@nats-client.%s.svc:4222",
		demo.Spec.NatsPassword,
		demo.Namespace,
	)

	nc, err := nats.Connect(url,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		logger.Error(err, "error connecting to nats")
		return
	}
	defer nc.Close()

	runNatsSubscriber(
		ctx,
		logger,
		client.ObjectKeyFromObject(demo).String(),
		nc,
	)
}

func runNatsSubscriber(
	ctx context.Context,
	logger logr.Logger,
	opniSystem string,
	nc *nats.Conn,
) {
	logger.Info("subscribed to forecasted_cpu_usage")
	ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		logger.Error(err, "error creating nats encoded connection")
		return
	}
	defer ec.Close()

	subCPUUsage, err := ec.Subscribe("forecasted_cpu_usage", func(msg *ForecastedCPUUsage) {
		logger.Info("Received forecasted_cpu_usage update")
		forecastedCPUUsage.With(prometheus.Labels{
			"opni":     opniSystem,
			"instance": msg.Instance,
		}).Set(msg.Value)
	})
	if err != nil {
		logger.Error(err, "error subscribing to forecasted_cpu_usage")
		return
	}
	defer subCPUUsage.Drain()

	subBounds, err := ec.Subscribe("forecasted_metric_bounds", func(msg *ForecastedMetricBounds) {
		logger.Info("Received forecasted_metric_bounds update")
		forecastedLowerBound.With(prometheus.Labels{
			"opni":        opniSystem,
			"metric_name": msg.MetricName,
		}).Set(msg.YhatLower)
		forecastedUpperBound.With(prometheus.Labels{
			"opni":        opniSystem,
			"metric_name": msg.MetricName,
		}).Set(msg.YhatUpper)
	})
	if err != nil {
		logger.Error(err, "error subscribing to forecasted_metric_bounds")
		return
	}
	defer subBounds.Drain()

	subCPUBacklog, err := ec.Subscribe("opni_nulog_cpu", func(msg *Backlog) {
		logger.Info("Received CPU backlog update")
		cpuBacklog.With(prometheus.Labels{
			"opni": opniSystem,
		}).Set(msg.BacklogCount)
	})
	if err != nil {
		logger.Error(err, "error subscribing to CPU backlog")
		return
	}
	defer subCPUBacklog.Drain()

	subGPUBacklog, err := ec.Subscribe("opni_nulog_gpu", func(msg *Backlog) {
		logger.Info("Received GPU backlog update")
		gpuBacklog.With(prometheus.Labels{
			"opni": opniSystem,
		}).Set(msg.BacklogCount)
	})
	if err != nil {
		logger.Error(err, "error subscribing to GPU backlog")
		return
	}
	defer subGPUBacklog.Drain()

	<-ctx.Done()
	logger.Info("Exiting nats subscriber")
}

func ReconcileServiceMonitor(
	ctx context.Context,
	mgr ctrl.Manager,
	logger logr.Logger,
) (bool, error) {
	cli := mgr.GetClient()
	var self appsv1.Deployment
	ns := os.Getenv("DEPLOYMENT_NAMESPACE")
	if ns == "" {
		logger.Info("Skipping service monitor creation, downward api not configured")
		return true, nil
	}
	err := cli.Get(ctx, types.NamespacedName{
		Name:      "opni-controller-manager",
		Namespace: ns,
	}, &self)
	if err != nil {
		logger.Error(err, "Skipping service monitor creation, downward api not configured")
		return true, nil
	}

	svcMonitor := &monitoringv1.ServiceMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "opni-metrics-monitor",
			Namespace: ns,
			Labels: map[string]string{
				"control-plane": "controller-manager",
			},
		},
		Spec: monitoringv1.ServiceMonitorSpec{
			Endpoints: []monitoringv1.Endpoint{
				{
					Port: "8080",
					Path: "/metrics",
				},
			},
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"control-plane": "controller-manager",
				},
			},
			NamespaceSelector: monitoringv1.NamespaceSelector{
				MatchNames: []string{ns},
			},
		},
	}

	// Create the service monitor
	if err := cli.Get(ctx, client.ObjectKeyFromObject(svcMonitor), svcMonitor); errors.IsNotFound(err) {
		logger.Info("creating resource", "name", client.ObjectKeyFromObject(svcMonitor))
		err := cli.Create(ctx, svcMonitor)
		if err != nil {
			logger.Error(err, "error creating resource", "name", client.ObjectKeyFromObject(svcMonitor))
			return false, nil
		}
	} else if runtime.IsNotRegisteredError(err) {
		logger.Info("Skipping service monitor creation, ensure prometheus operator is installed and restart the opni manager",
			"error", err.Error(),
		)
		return false, err
	} else if err != nil {
		logger.Error(err, "error looking up service monitor")
		return false, err
	}

	return true, nil
}
