# AMQ Helm Chart

This Helm chart deploys the Agentic Message Queue (AMQ) on a Kubernetes cluster.

## Installing the Chart

To install the chart with the release name `my-amq`:

```bash
helm install my-amq ./helm/amq
```
## Uninstalling the Chart

To uninstall/delete the `my-amq` deployment:

```bash
helm delete my-amq
```

## Configuration

The following table lists the configurable parameters and their default values.

| Parameter | Description | Default |
| --------- | ----------- | ------- |
| `replicaCount` | Number of AMQ replicas | `3` |
| `image.repository` | AMQ image repository | `rizome/amq` |
| `image.tag` | AMQ image tag | `""` (uses appVersion) |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `imagePullSecrets` | Image pull secrets | `[]` |
| `nameOverride` | Override the app name | `""` |
| `fullnameOverride` | Override the full app name | `""` |
| `serviceAccount.create` | Create a service account | `true` |
| `serviceAccount.annotations` | Service account annotations | `{}` |
| `serviceAccount.name` | Service account name | `""` |
| `podAnnotations` | Pod annotations | `{}` |
| `podSecurityContext` | Pod security context | See values.yaml |
| `securityContext` | Container security context | See values.yaml |
| `service.type` | Service type | `ClusterIP` |
| `service.port` | Service port | `8080` |
| `service.annotations` | Service annotations | `{}` |
| `config.storePath` | Path to store BadgerDB data | `/data/amq` |
| `config.workerPoolSize` | Number of workers per queue | `10` |
| `config.heartbeatInterval` | Agent heartbeat interval | `30s` |
| `config.messageTimeout` | Default message processing timeout | `5m` |
| `config.logLevel` | Log level | `info` |
| `persistence.enabled` | Enable persistence | `true` |
| `persistence.storageClass` | Storage class | `""` (default) |
| `persistence.accessMode` | Access mode | `ReadWriteOnce` |
| `persistence.size` | Storage size | `10Gi` |
| `persistence.annotations` | PVC annotations | `{}` |
| `resources.limits` | Resource limits | `{cpu: 1000m, memory: 2Gi}` |
| `resources.requests` | Resource requests | `{cpu: 500m, memory: 1Gi}` |
| `autoscaling.enabled` | Enable HPA | `false` |
| `autoscaling.minReplicas` | Minimum replicas | `3` |
| `autoscaling.maxReplicas` | Maximum replicas | `10` |
| `autoscaling.targetCPUUtilizationPercentage` | Target CPU usage | `80` |
| `autoscaling.targetMemoryUtilizationPercentage` | Target memory usage | `80` |
| `nodeSelector` | Node selector | `{}` |
| `tolerations` | Tolerations | `[]` |
| `affinity` | Affinity rules | See values.yaml |
| `monitoring.enabled` | Enable monitoring | `false` |
| `monitoring.serviceMonitor.enabled` | Create ServiceMonitor | `false` |
| `monitoring.serviceMonitor.interval` | Scrape interval | `30s` |
| `monitoring.serviceMonitor.path` | Metrics path | `/metrics` |
| `networkPolicy.enabled` | Enable NetworkPolicy | `false` |
| `podDisruptionBudget.enabled` | Enable PDB | `true` |
| `podDisruptionBudget.minAvailable` | Minimum available pods | `2` |

### Specifying Values

Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example:

```bash
helm install my-amq ./helm/amq \
  --set persistence.size=50Gi \
  --set replicaCount=5
```

Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart:

```bash
helm install my-amq ./helm/amq -f values.yaml
```

## Persistence

The AMQ chart mounts a Persistent Volume for BadgerDB data storage. The volume is created using dynamic volume provisioning. You can disable persistence by setting `persistence.enabled` to `false`.

## High Availability

The chart deploys AMQ as a StatefulSet with multiple replicas for high availability. Each replica maintains its own BadgerDB instance. For production deployments, it's recommended to:

1. Use at least 3 replicas
2. Enable pod anti-affinity (enabled by default)
3. Enable PodDisruptionBudget (enabled by default)
4. Use persistent storage with appropriate backup strategies

## Monitoring

When monitoring is enabled, AMQ exposes Prometheus metrics at `/metrics`. If you're using Prometheus Operator, you can enable the ServiceMonitor creation:

```yaml
monitoring:
  enabled: true
  serviceMonitor:
    enabled: true
```

## Security

The default security context runs the container as a non-root user with a read-only root filesystem. You can customize the security settings in `values.yaml`.

## Upgrading

To upgrade an existing AMQ deployment:

```bash
helm upgrade my-amq ./helm/amq
```

Note: When upgrading, ensure you review the release notes for any breaking changes.

## Troubleshooting

### Pods stuck in Pending state

Check if PersistentVolumeClaims are being provisioned:

```bash
kubectl get pvc -l app.kubernetes.io/instance=my-amq
```

### High memory usage

Adjust the BadgerDB cache settings or increase resource limits:

```yaml
resources:
  limits:
    memory: 4Gi
```

### Connection issues

Ensure your agents are connecting to the correct service endpoint:

```
my-amq.namespace.svc.cluster.local:8080
```
