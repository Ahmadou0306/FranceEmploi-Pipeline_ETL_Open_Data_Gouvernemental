#!/bin/bash

ROLES=(
  "roles/iam.roleAdmin"
  "roles/iam.serviceAccountAdmin"
  "roles/monitoring.admin"
  "roles/pubsub.admin"
  "roles/resourcemanager.projectIamAdmin"
  "roles/storage.admin"
)

echo "Entrez le project ID (ex: project-gcp-59684) :"
read PROJECT_ID

echo "Entrez l'adresse du service account (ex: dudhdks@awseuwest3-da77.iam.gserviceaccount.com) :"
read MEMBER

for ROLE in "${ROLES[@]}"; do
  echo "Attribution de $ROLE..."
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$MEMBER" \
    --role="$ROLE"
done

echo "Terminé. Vérification :"
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:$MEMBER" \
  --format="table(bindings.role)"