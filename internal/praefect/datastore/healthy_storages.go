package datastore

// HealthyStoragesQuery returns the SQL to determine whether a node is up.
// The view in internal/praefect/datastore/migrations/20210525143540_healthy_storages_view.go
// can be dropped once this is deployed.
func HealthyStoragesQuery() string {
	return `
SELECT shard_name AS virtual_storage, node_name AS storage
FROM node_status AS ns
WHERE last_seen_active_at >= NOW() - INTERVAL '10 SECOND'
GROUP BY shard_name, node_name
HAVING COUNT(praefect_name) >= (
	SELECT CEIL(COUNT(DISTINCT praefect_name) / 2.0) AS quorum_count
	FROM node_status
	WHERE shard_name = ns.shard_name
	AND last_contact_attempt_at >= NOW() - INTERVAL '60 SECOND'
)
ORDER BY shard_name, node_name`
}
