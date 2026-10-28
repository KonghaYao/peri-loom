import { useEffect, useRef, useState, type FormEvent } from 'react';
import { Button } from '@cloudflare/kumo/components/button';
import { Input } from '@cloudflare/kumo/components/input';
import { Select } from '@cloudflare/kumo/components/select';
import type { Call } from '../../App';
import type { Catalog } from '../../api';
import { ErrorNotice, Modal } from '../../app/ui';
import {
  creationFailure,
  defaultRegion,
  normalizeLabel,
  organizationIDs,
  projectCreateBody,
  projectTenant,
  validateProjectDraft,
} from './project-create';

type CreateProjectProps = {
  open: boolean;
  call: Call;
  catalog: Catalog;
  refresh: () => Promise<void>;
  organizations?: string[];
  initialOrganization?: string;
  onClose: () => void;
  onCreated: (id: string) => void;
};

export function CreateProject({ open, call, catalog, refresh, organizations = [], initialOrganization = '', onClose, onCreated }: CreateProjectProps) {
  const known = organizationIDs(catalog, organizations);
  const [organization, setOrganization] = useState('');
  const [manualOrganization, setManualOrganization] = useState(false);
  const [projectID, setProjectID] = useState('');
  const [region, setRegion] = useState('');
  const [error, setError] = useState('');
  const [created, setCreated] = useState('');
  const [busy, setBusy] = useState(false);
  const submitting = useRef(false);

  useEffect(() => {
    if (!open) return;
    const first = initialOrganization && known.includes(initialOrganization) ? initialOrganization : known[0] ?? '';
    const preferred = Object.values(catalog.projects).find(item => item.organization === first)?.region ?? '';
    setOrganization(first);
    setManualOrganization(known.length === 0);
    setProjectID('');
    setRegion(defaultRegion(catalog, preferred));
    setError('');
    setCreated('');
    setBusy(false);
    submitting.current = false;
    // The form is a fresh draft each time the dialog opens; catalog changes
    // while it is closed must not leak into the next create.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const close = () => {
    if (busy) return;
    onClose();
  };

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (submitting.current || created) return;
    const draft = { id: normalizeLabel(projectID), organization: normalizeLabel(organization), region: normalizeLabel(region), manual: manualOrganization };
    const validation = validateProjectDraft(draft, catalog, known);
    if (validation) {
      setError(validation);
      return;
    }
    submitting.current = true;
    setBusy(true);
    setError('');
    try {
      await call('/v1/projects/create', projectCreateBody(draft));
    } catch (failure) {
      setError(creationFailure(failure, 'project').text);
      setBusy(false);
      submitting.current = false;
      return;
    }
    let refreshFailure: unknown = null;
    try {
      await refresh();
    } catch (failure) {
      refreshFailure = failure;
    }
    setBusy(false);
    submitting.current = false;
    setCreated(draft.id);
    if (refreshFailure) {
      // Creation is already durable; presenting it as a failure would invite a
      // duplicate submit, and navigating without a refreshed catalog would land
      // on an unknown project.
      setError(`项目“${draft.id}”已创建，但目录刷新失败：${String(refreshFailure)}。请刷新页面确认，勿重复提交。`);
      return;
    }
    onCreated(draft.id);
  };

  return <Modal open={open} onClose={close} title="新建项目" footer={<Button form="create-project-form" type="submit" variant="primary" disabled={busy || !!created || !organization.trim() || !projectID.trim() || !region.trim()}>{created ? '已创建' : busy ? '创建中…' : '新建项目'}</Button>}>
    <form id="create-project-form" onSubmit={submit} className="stack">
      {manualOrganization || !known.length
        ? <Input label="组织 ID" value={organization} onChange={event => setOrganization(event.target.value)} required description="1–64 位字母、数字、下划线或连字符；组织必须已存在。" />
        : <Select label="组织" value={organization} onValueChange={value => { if (typeof value === 'string') setOrganization(value); }} description="从可见项目推导，不代表完整组织列表。">
            {known.map(id => <Select.Option key={id} value={id}>{id}</Select.Option>)}
          </Select>}
      {known.length > 0 && <Button type="button" variant="ghost" onClick={() => setManualOrganization(value => !value)}>{manualOrganization ? '选择已有组织' : '找不到组织？手动输入组织 ID'}</Button>}
      {!known.length && <p className="muted small">还没有从项目中发现组织；请先新建组织，或手动输入组织 ID。手动输入的组织由服务端校验。</p>}
      <Input label="项目 ID" value={projectID} onChange={event => setProjectID(event.target.value)} required autoFocus description="1–64 位字母、数字、下划线或连字符；创建后不可修改。" />
      <Input label="部署区域" value={region} onChange={event => setRegion(event.target.value)} required description="默认沿用现有项目的区域；必须与平台 Worker 的区域一致。" />
      {organization.trim() && projectID.trim() && <p className="muted small mono">租户标识：{projectTenant({ id: projectID, organization })}</p>}
      <p className="muted small">项目使用平台默认配额；创建后可在“项目设置”查看。</p>
      <ErrorNotice error={error} />
    </form>
  </Modal>;
}
