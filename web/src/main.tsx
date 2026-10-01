import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter } from 'react-router-dom';
import { AuthProvider } from './hooks/useAuth';
import { ApiError } from './api/client';
import App from './App';
import './styles.css';

// React Query：轻量集中式服务端状态（列表 / 详情 / 轮询）
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5_000,
      refetchOnWindowFocus: false,
      // 只对明确可重试的错误重试，避免 4xx 无意义重放
      retry: (failureCount, error) => {
        if (error instanceof ApiError) return error.retryable && failureCount < 2;
        return failureCount < 1;
      },
    },
  },
});

const container = document.getElementById('root');
if (!container) throw new Error('#root 容器不存在');

createRoot(container).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AuthProvider>
          <App />
        </AuthProvider>
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
);
