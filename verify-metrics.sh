#!/bin/bash

# Script to verify metrics are being exposed correctly

echo "=== Verifying Metrics Setup ==="
echo ""

# Check if metrics endpoint is accessible
echo "1. Checking metrics endpoint (http://localhost:8081/metrics)..."
if curl -s http://localhost:8081/metrics > /dev/null 2>&1; then
    echo "   ✓ Metrics endpoint is accessible"
    METRICS_COUNT=$(curl -s http://localhost:8081/metrics | grep -c "citibike_")
    echo "   Found $METRICS_COUNT citibike metrics"
    
    echo ""
    echo "   Sample metrics:"
    curl -s http://localhost:8081/metrics | grep "citibike_" | head -5
else
    echo "   ✗ Metrics endpoint is NOT accessible"
    echo "   Make sure your application is running and metrics server started"
fi

echo ""
echo "2. Checking Prometheus target..."
if curl -s http://localhost:9090/api/v1/targets | grep -q "citibike-app"; then
    echo "   ✓ Prometheus knows about citibike-app target"
    
    # Check target status
    STATUS=$(curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"[^"]*"' | head -1 | cut -d'"' -f4)
    echo "   Target health: $STATUS"
    
    if [ "$STATUS" != "up" ]; then
        echo "   ⚠ Warning: Target is not UP"
        echo "   Check Prometheus logs: docker logs prometheus"
    fi
else
    echo "   ✗ Prometheus target not found"
fi

echo ""
echo "3. Checking if Prometheus can scrape metrics..."
if curl -s "http://localhost:9090/api/v1/query?query=citibike_locations_count" | grep -q "result"; then
    echo "   ✓ Prometheus can query citibike metrics"
    
    # Show a sample query result
    echo ""
    echo "   Sample query result (citibike_locations_count):"
    curl -s "http://localhost:9090/api/v1/query?query=citibike_locations_count" | python3 -m json.tool 2>/dev/null || curl -s "http://localhost:9090/api/v1/query?query=citibike_locations_count"
else
    echo "   ✗ Prometheus cannot query citibike metrics"
    echo "   This usually means:"
    echo "   - Metrics haven't been scraped yet (wait 15-30 seconds)"
    echo "   - Prometheus can't reach the metrics endpoint"
    echo "   - Check prometheus.yml configuration"
fi

echo ""
echo "4. Checking Grafana datasource..."
if curl -s -u admin:admin http://localhost:3000/api/datasources | grep -q "prometheus"; then
    echo "   ✓ Grafana has Prometheus datasource configured"
else
    echo "   ✗ Grafana datasource not found"
fi

echo ""
echo "=== Troubleshooting Tips ==="
echo "1. Make sure your Java application is running"
echo "2. Verify metrics endpoint: curl http://localhost:8081/metrics"
echo "3. Check Prometheus targets: http://localhost:9090/targets"
echo "4. Verify Prometheus config: docker exec prometheus cat /etc/prometheus/prometheus.yml"
echo "5. Check Prometheus logs: docker logs prometheus"
echo "6. On Linux, you may need to use host IP instead of host.docker.internal"
echo ""

