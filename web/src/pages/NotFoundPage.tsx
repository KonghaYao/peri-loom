/** 404 页面 */
import { Button, Result } from 'antd';
import { Link } from 'react-router-dom';

export function NotFoundPage(): JSX.Element {
  return (
    <Result
      status="404"
      title="404"
      subTitle="页面不存在"
      extra={
        <Link to="/dashboard">
          <Button type="primary">返回概览</Button>
        </Link>
      }
    />
  );
}
