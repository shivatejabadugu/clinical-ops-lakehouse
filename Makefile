.PHONY: data test local deploy upload run clean

data:        ## generate 45 days of synthetic FHIR exports
	python -m generator.generate_fhir --days 45

test:        ## run the pipeline SQL on local Spark against hand-built and generated data
	pytest -q

local: data  ## run the full pipeline locally and print the gold tables
	python -m local.run_local

deploy:      ## deploy schemas, volume, pipeline and job to Databricks (dev)
	databricks bundle deploy -t dev

upload:      ## upload all export batches to the landing volume
	scripts/upload_batches.sh

run:         ## refresh the pipeline, apply governance, run the quality gate
	databricks bundle run clinical_ops_refresh -t dev

clean:
	rm -rf data .pytest_cache
