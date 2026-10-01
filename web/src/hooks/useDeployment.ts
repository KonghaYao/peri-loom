import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import type { DeploymentInfo } from '../api/types';

/** 能力以服务端为准；页面在首次加载完成后挂载，避免误发集群请求。 */
export function useDeployment() {
  return useQuery<DeploymentInfo>({
    queryKey: ['deployment'],
    queryFn: api.deployment.get,
    staleTime: 60_000,
    retry: false,
  });
}
