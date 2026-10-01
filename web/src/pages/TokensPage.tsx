import { Card } from 'antd';
import { TokenManager } from '../components/tokens/TokenManager';

export function TokensPage(): JSX.Element {
  return <Card title="API 凭据"><TokenManager /></Card>;
}
