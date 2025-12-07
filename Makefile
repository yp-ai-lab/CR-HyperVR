PY ?= python3

.PHONY: install gcp-verify gcp-verify-py gcp-build gcp-deploy gcp-deploy-infra db-apply-cloudsql gcp-jobs-deploy gcp-secrets pipeline-phase2 upload-gcs-assets gcp-log gcp-logged-% build-hyperedges validate-hyperedges

install:
	$(PY) -m pip install -r requirements.txt

gcp-verify:
	bash scripts/gcp_verify.sh

gcp-verify-py:
	$(PY) scripts/verify_gcp_access.py

gcp-build:
	gcloud builds submit --region=$${REGION:-europe-west2} --tag $${REGION:-europe-west2}-docker.pkg.dev/$${PROJECT_ID}/$${AR_REPO:-embedding-service}/api:latest .

gcp-deploy:
	bash scripts/deploy_cloud_run.sh

gcp-deploy-infra:
	bash scripts/deploy_graph_service.sh

db-apply-cloudsql:
	bash scripts/db_apply_cloudsql.sh

gcp-jobs-deploy:
	bash scripts/deploy_jobs.sh

gcp-secrets:
	bash scripts/setup_secrets.sh

pipeline-phase2:
	PYTHONPATH=. $(PY) scripts/run_pipeline_phase2.py

upload-gcs-assets:
	bash scripts/upload_gcs_assets.sh

gcp-log:
	@[ -z "$(PURPOSE)" ] && echo "PURPOSE is required" && exit 1 || true
	@[ -z "$(CMD)" ] && echo "CMD is required" && exit 1 || true
	$(PY) scripts/gcp_log.py --executor "$${EXECUTOR:-Make}" --purpose "$(PURPOSE)" --run "$(CMD)"

gcp-logged-%:
	$(PY) scripts/gcp_log.py --executor "Make" --purpose "$${PURPOSE:-make $*}" --run "$(MAKE) $*"

build-hyperedges:
	PYTHONPATH=. $(PY) scripts/build_hyperedges.py

validate-hyperedges:
	PYTHONPATH=. $(PY) scripts/validate_hyperedges.py

