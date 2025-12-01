#!/bin/bash

echo "========================================="
echo "BIGQUERY EXTERNAL TABLES VALIDATION"
echo "========================================="

# Arrays of datasets and tables
declare -a bronze_tables=("transactions" "job_control" "quarantine")
declare -a silver_tables=("transactions" "job_control" "quarantine")
declare -a gold_tables=("fact_transactions" "dim_customers" "dim_merchants" "dim_payment_methods" "dim_transaction_status" "dim_date" "job_control")

total_tables=0
passed=0
failed=0

# Function to test table
test_table() {
    local dataset=$1
    local table=$2
    
    echo -n "Testing ${dataset}.${table}... "
    
    result=$(bq query --use_legacy_sql=false --format=csv \
        "SELECT COUNT(*) as cnt FROM \`grand-jigsaw-476820-t1.${dataset}.${table}\`" 2>&1)
    
    if echo "$result" | grep -q "cnt"; then
        echo "✅ PASS (queryable)"
        ((passed++))
    else
        echo "❌ FAIL"
        echo "   Error: $result"
        ((failed++))
    fi
    
    ((total_tables++))
}

# Test Bronze tables
echo ""
echo "Testing Bronze Dataset..."
echo "-----------------------------------------"
for table in "${bronze_tables[@]}"; do
    test_table "bronze_dataset" "$table"
done

# Test Silver tables
echo ""
echo "Testing Silver Dataset..."
echo "-----------------------------------------"
for table in "${silver_tables[@]}"; do
    test_table "silver_dataset" "$table"
done

# Test Gold tables
echo ""
echo "Testing Gold Dataset..."
echo "-----------------------------------------"
for table in "${gold_tables[@]}"; do
    test_table "gold_dataset" "$table"
done

# Summary
echo ""
echo "========================================="
echo "VALIDATION SUMMARY"
echo "========================================="
echo "Total tables tested: $total_tables"
echo "Passed: $passed"
echo "Failed: $failed"
echo ""

if [ $failed -eq 0 ]; then
    echo "✅ ALL BIGQUERY EXTERNAL TABLES VALIDATED"
    exit 0
else
    echo "❌ SOME TABLES FAILED VALIDATION"
    exit 1
fi