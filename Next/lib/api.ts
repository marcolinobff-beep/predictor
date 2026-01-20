import type {
  AnalyzeRequest,
  AnalyzeResponse,
  SlateResponse,
  OddsRiskRequest,
  OddsRiskResponse,
  PlayersRequest,
  PlayersResponse,
  AuditHistoryResponse,
  AuditDetailResponse,
  DashboardKPIs,
  ChatRequest,
  ChatResponse,
  ApiError,
} from './types';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000';

// Get token from localStorage
function getToken(): string | null {
  if (typeof window === 'undefined') return null;
  return localStorage.getItem('auth_token');
}

// Set token in localStorage
export function setToken(token: string): void {
  if (typeof window !== 'undefined') {
    localStorage.setItem('auth_token', token);
  }
}

// Remove token from localStorage
export function removeToken(): void {
  if (typeof window !== 'undefined') {
    localStorage.removeItem('auth_token');
  }
}

// Check if user is authenticated
export function isAuthenticated(): boolean {
  return !!getToken();
}

// Generic fetch wrapper with auth
async function apiFetch<T>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const token = getToken();
  
  if (!token) {
    throw new Error('Non autenticato. Effettua il login.');
  }

  const headers: HeadersInit = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}`,
    ...options.headers,
  };

  const response = await fetch(`${API_BASE_URL}${endpoint}`, {
    ...options,
    headers,
  });

  if (!response.ok) {
    const error: ApiError = await response.json().catch(() => ({
      error: 'API Error',
      message: `Errore ${response.status}: ${response.statusText}`,
      status: response.status,
    }));
    throw new Error(error.message || `Errore API: ${response.status}`);
  }

  return response.json();
}

// Dashboard API
export async function getDashboardKPIs(): Promise<DashboardKPIs> {
  return apiFetch<DashboardKPIs>('/v1/ui/dashboard/kpis');
}

// Analyze API
export async function analyzeMatch(request: AnalyzeRequest): Promise<AnalyzeResponse> {
  return apiFetch<AnalyzeResponse>('/v1/ui/analyze', {
    method: 'POST',
    body: JSON.stringify(request),
  });
}

export async function analyzeById(matchId: string): Promise<AnalyzeResponse> {
  return apiFetch<AnalyzeResponse>(`/v1/ui/analyze_by_id/${matchId}`);
}

// Slate API
export async function getSlate(
  date?: string,
  competition?: string
): Promise<SlateResponse> {
  const params = new URLSearchParams();
  if (date) params.append('date', date);
  if (competition) params.append('competition', competition);
  
  const query = params.toString() ? `?${params.toString()}` : '';
  return apiFetch<SlateResponse>(`/v1/ui/slate${query}`);
}

// Odds Risk API
export async function getOddsRisk(
  filters: OddsRiskRequest = {}
): Promise<OddsRiskResponse> {
  return apiFetch<OddsRiskResponse>('/v1/ui/odds-risk', {
    method: 'POST',
    body: JSON.stringify(filters),
  });
}

// Players API
export async function getPlayerProjections(
  request: PlayersRequest = {}
): Promise<PlayersResponse> {
  const params = new URLSearchParams();
  if (request.team) params.append('team', request.team);
  if (request.match_id) params.append('match_id', request.match_id);
  if (request.competition) params.append('competition', request.competition);
  
  const query = params.toString() ? `?${params.toString()}` : '';
  return apiFetch<PlayersResponse>(`/v1/ui/players/projections${query}`);
}

// Audit API
export async function getAuditHistory(
  page: number = 1,
  perPage: number = 20
): Promise<AuditHistoryResponse> {
  return apiFetch<AuditHistoryResponse>(
    `/v1/ui/audit/history?page=${page}&per_page=${perPage}`
  );
}

export async function getAuditDetail(runId: string): Promise<AuditDetailResponse> {
  return apiFetch<AuditDetailResponse>(`/v1/ui/audit/${runId}`);
}

// Chat API
export async function sendChatMessage(
  request: ChatRequest
): Promise<ChatResponse> {
  return apiFetch<ChatResponse>('/v1/chat', {
    method: 'POST',
    body: JSON.stringify(request),
  });
}
