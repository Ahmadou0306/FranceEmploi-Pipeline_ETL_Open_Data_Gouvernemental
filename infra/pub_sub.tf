# -------------------------------------------------------
# TOPICS PUB/SUB
# -------------------------------------------------------
resource "google_pubsub_topic" "snowpipe_topics" {
  for_each = local.flux
  name     = each.value.topic_name
}

# -------------------------------------------------------
# SUBSCRIPTIONS PUB/SUB
# -------------------------------------------------------
resource "google_pubsub_subscription" "snowpipe_subs" {
  for_each = local.flux

  name  = "${each.value.topic_name}-sub"
  topic = google_pubsub_topic.snowpipe_topics[each.key].name

  ack_deadline_seconds = 60

  expiration_policy {
    ttl = ""
  }
}

# -------------------------------------------------------
# DÉLAI pour laisser l'IAM se propager
# -------------------------------------------------------
resource "time_sleep" "wait_iam_propagation" {
  depends_on = [google_pubsub_topic_iam_member.gcs_publisher]

  create_duration = "30s"
}

# -------------------------------------------------------
# NOTIFICATIONS GCS → PUB/SUB
# -------------------------------------------------------
resource "google_storage_notification" "snowpipe_notifications" {
  for_each = local.flux

  bucket             = google_storage_bucket.raw_data_bucket.name
  topic              = "projects/${var.project_id}/topics/${each.value.topic_name}" # ← chemin explicite
  payload_format     = "JSON_API_V1"
  event_types        = ["OBJECT_FINALIZE"]
  object_name_prefix = each.value.object_prefix

  depends_on = [
    google_pubsub_topic.snowpipe_topics,
    google_pubsub_topic_iam_member.gcs_publisher
  ]
}

# -------------------------------------------------------
# OUTPUT
# -------------------------------------------------------
output "subscription_names" {
  description = "Noms des subscriptions à renseigner dans Snowflake"
  value = {
    for k, sub in google_pubsub_subscription.snowpipe_subs :
    k => "projects/${var.project_id}/subscriptions/${sub.name}"
  }
}