/**
 * 数据库相关弹窗：创建 / 迁移 / 恢复（需要额外输入的长操作）。
 * 提交成功后由调用方拿到 operation_id 并轮询进度。
 */
import { useState } from 'react';
import { Alert, App as AntdApp, Form, Input, Modal, Select, Space, Typography } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { useDeployment } from '../../hooks/useDeployment';
import { api, toItems } from '../../api/client';
import { ErrorAlert } from '../ErrorAlert';
import { workerSaturation, workerState } from '../../utils/worker';
import { WorkerStateTag } from '../StatusTag';
import { formatBytes, formatTime } from '../../utils/format';
import { snapshotDbId } from '../../utils/database';

interface BaseModalProps {
  open: boolean;
  onClose: () => void;
  /** 提交成功（202）后返回 operation_id */
  onSubmitted: (operationId: string) => void;
}

// ---------------------------------------------------------------- 创建数据库

interface CreateForm {
  name: string;
}

export function CreateDatabaseModal({ open, onClose, onSubmitted }: BaseModalProps): JSX.Element {
  const [form] = Form.useForm<CreateForm>();
  const { message } = AntdApp.useApp();
  const [error, setError] = useState<unknown>(null);
  const [submitting, setSubmitting] = useState(false);

  const handleOk = async () => {
    const values = await form.validateFields();
    setSubmitting(true);
    setError(null);
    try {
      // 不再指定归属租户：由服务端按调用主体缺省填充（既有行为）
      const accepted = await api.databases.create({ name: values.name.trim() });
      form.resetFields();
      // operation_id 为 null 表示幂等无操作（目标状态已达成），此时没有可跟踪的操作
      if (accepted.operation_id) {
        message.success('创建请求已受理');
        onSubmitted(accepted.operation_id);
      } else {
        message.info('目标状态已达成，未产生新操作');
      }
      onClose();
    } catch (err) {
      setError(err);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      open={open}
      title="创建数据库"
      onCancel={() => {
        setError(null);
        onClose();
      }}
      onOk={handleOk}
      okText="创建"
      confirmLoading={submitting}
      destroyOnClose
    >
      <ErrorAlert error={error} />
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 16 }}
        message="连接信息会在数据库创建成功后显示"
        description="libSQL 与 TursoDB SDK 共用当前 Panel 的主机和端口，通过数据库路径区分实例；不会为新库单独开放端口。数据库创建成功后，请前往该库详情页申请本库 Token；明文只在申请成功后显示一次。"
      />
      <Form<CreateForm> form={form} layout="vertical" preserve={false}>
        <Form.Item
          name="name"
          label="数据库名称"
          rules={[
            { required: true, message: '请输入数据库名称' },
            { pattern: /^[A-Za-z0-9_-]{1,64}$/, message: '仅支持字母、数字、下划线、连字符（1-64 位）' },
          ]}
        >
          <Input placeholder="例如：orders_db" autoComplete="off" />
        </Form.Item>
      </Form>
      <Typography.Text type="secondary">
        创建是异步长操作：提交后返回 operation_id，可在进度弹窗或「操作中心」跟踪。操作成功后，进度弹窗会展示 SDK 地址，并提供前往数据库详情申请本库 Token 的链接。
      </Typography.Text>
    </Modal>
  );
}

// ---------------------------------------------------------------- 迁移

export function MoveDatabaseModal({
  open,
  dbId,
  onClose,
  onSubmitted,
}: BaseModalProps & { dbId: string }): JSX.Element {
  const { message } = AntdApp.useApp();
  const [error, setError] = useState<unknown>(null);
  const [submitting, setSubmitting] = useState(false);
  const [target, setTarget] = useState<string | undefined>(undefined);

  const canMove = useDeployment().data?.capabilities.database_move ?? false;
  const workersQuery = useQuery({
    queryKey: ['workers'],
    queryFn: () => api.workers.list(),
    enabled: open && canMove,
  });
  const workers = toItems(workersQuery.data);

  const handleOk = async () => {
    setSubmitting(true);
    setError(null);
    try {
      const accepted = await api.databases.move(dbId, target ? { target_worker_id: target } : {});
      if (accepted.operation_id) {
        message.success('迁移请求已受理');
        onSubmitted(accepted.operation_id);
      } else {
        message.info('目标状态已达成，未产生新操作');
      }
      onClose();
    } catch (err) {
      setError(err);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      open={open}
      title={`迁移数据库 ${dbId}`}
      onCancel={() => {
        setError(null);
        onClose();
      }}
      onOk={handleOk}
      okText="开始迁移"
      confirmLoading={submitting}
      width={560}
    >
      <ErrorAlert error={error} />
      <ErrorAlert error={workersQuery.error} onRetry={() => workersQuery.refetch()} />
      <Space direction="vertical" size={8} style={{ width: '100%' }}>
        <Typography.Text>目标 Worker（留空则由调度器选择）：</Typography.Text>
        <Select
          style={{ width: '100%' }}
          allowClear
          placeholder="自动选择"
          value={target}
          onChange={setTarget}
          loading={workersQuery.isLoading}
          options={workers.map((w) => ({
            value: w.id,
            label: (
              <Space>
                <span>{w.id}</span>
                <WorkerStateTag state={workerState(w)} />
                <span style={{ fontSize: 12, color: '#8c8c8c' }}>
                  饱和度 {workerSaturation(w) === null ? '无数据' : `${workerSaturation(w)!.toFixed(0)}%`}
                </span>
              </Space>
            ),
          }))}
        />
        <Typography.Text type="secondary">
          迁移会切换 Owner Worker 并递增 Epoch；迁移期间数据库通常短暂不可写。
        </Typography.Text>
      </Space>
    </Modal>
  );
}

// ---------------------------------------------------------------- 恢复

export function RestoreDatabaseModal({
  open,
  dbId,
  onClose,
  onSubmitted,
}: BaseModalProps & { dbId: string }): JSX.Element {
  const { message } = AntdApp.useApp();
  const [error, setError] = useState<unknown>(null);
  const [submitting, setSubmitting] = useState(false);
  const [snapshotId, setSnapshotId] = useState<string | undefined>(undefined);

  const snapshotsQuery = useQuery({
    queryKey: ['snapshots', dbId],
    queryFn: () => api.snapshots.list(dbId),
    enabled: open,
  });
  const snapshots = toItems(snapshotsQuery.data).filter((s) => snapshotDbId(s) === undefined || snapshotDbId(s) === dbId);

  const handleOk = async () => {
    setSubmitting(true);
    setError(null);
    try {
      const accepted = await api.databases.restore(dbId, snapshotId ? { snapshot_id: snapshotId } : {});
      if (accepted.operation_id) {
        message.success('恢复请求已受理');
        onSubmitted(accepted.operation_id);
      } else {
        message.info('目标状态已达成，未产生新操作');
      }
      onClose();
    } catch (err) {
      setError(err);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      open={open}
      title={`恢复数据库 ${dbId}`}
      onCancel={() => {
        setError(null);
        onClose();
      }}
      onOk={handleOk}
      okText="开始恢复"
      okButtonProps={{ danger: true }}
      confirmLoading={submitting}
      width={560}
    >
      <ErrorAlert error={error} />
      <ErrorAlert error={snapshotsQuery.error} onRetry={() => snapshotsQuery.refetch()} />
      <Space direction="vertical" size={8} style={{ width: '100%' }}>
        <Typography.Text type="warning">
          恢复会用快照覆盖当前数据，请确认已无写入流量。
        </Typography.Text>
        <Select
          style={{ width: '100%' }}
          allowClear
          placeholder="使用最新快照"
          value={snapshotId}
          onChange={setSnapshotId}
          loading={snapshotsQuery.isLoading}
          options={snapshots.map((s) => ({
            value: s.id,
            label: (
              <Space>
                <span>{s.id}</span>
                <span style={{ fontSize: 12, color: '#8c8c8c' }}>
                  {formatTime(s.created_at)}
                  {s.size_bytes !== undefined ? ` · ${formatBytes(s.size_bytes)}` : ''}
                </span>
              </Space>
            ),
          }))}
        />
        <Typography.Text type="secondary">
          {snapshots.length === 0
            ? '该数据库当前没有快照；可先执行「快照」动作。'
            : `共 ${snapshots.length} 个快照，留空则使用最新快照。`}
        </Typography.Text>
      </Space>
    </Modal>
  );
}
