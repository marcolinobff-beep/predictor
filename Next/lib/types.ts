// API Response Types for Football Prediction Bot

// Common types
export interface Match {
  id: string;
  home_team: string;
  away_team: string;
  competition: string;
  date: string;
  kickoff: string;
  status: 'scheduled' | 'live' | 'finished';
}

export interface Odds {
  home: number;
  draw: number;
  away: number;
  over_2_5: number;
  under_2_5: number;
  btts_yes: number;
  btts_no: number;
}

export interface FairOdds extends Odds {
  implied_prob_home: number;
  implied_prob_draw: number;
  implied_prob_away: number;
}

export interface Probability {
  outcome: string;
  probability: number;
  fair_odds: number;
  market_odds?: number;
  edge?: number;
  ev?: number;
}

export interface Scoreline {
  home_goals: number;
  away_goals: number;
  probability: number;
  cumulative?: number;
}

// Analyze endpoint types
export interface AnalyzeRequest {
  home_team: string;
  away_team: string;
  competition?: string;
  date?: string;
}

export interface AnalyzeResponse {
  match: Match;
  probabilities: {
    result_1x2: Probability[];
    over_under: Probability[];
    btts: Probability[];
  };
  fair_odds: FairOdds;
  edge_analysis: EdgeAnalysis[];
  top_scorelines: Scoreline[];
  audit_snapshot: AuditSnapshot;
  analysis_pro?: AnalysisPro;
}

export interface AnalysisPro {
  confidence?: ModelConfidence;
  drivers?: DriverImpact[];
  tactical?: TacticalInfo;
  lineup?: LineupInfo;
  form?: FormInfo;
  schedule?: ScheduleInfo;
  kpi?: KpiInfo;
  scenario?: ScenarioAnalysis;
}

export interface ModelConfidence {
  score?: number;
  lineup_coverage?: number;
  form_stability?: number;
  data_quality_score?: number;
}

export interface DriverImpact {
  key?: string;
  label?: string;
  home_delta?: number;
  away_delta?: number;
  home_delta_pct?: number;
  away_delta_pct?: number;
  note?: string;
}

export interface TacticalInfo {
  source?: string;
  tags?: string[];
  matchup?: string;
  tempo?: string;
  style_matchup?: {
    indicator?: string;
    home_edge?: string;
    away_edge?: string;
    reason?: string;
  };
}

export interface LineupInfo {
  lineup_source?: string;
  lineup_confidence?: number;
  coverage_home?: number;
  coverage_away?: number;
  absence_share_home?: number;
  absence_share_away?: number;
  penalty_home?: number;
  penalty_away?: number;
}

export interface FormInfo {
  xg_for_delta_home?: number;
  xg_for_delta_away?: number;
  xg_against_delta_home?: number;
  xg_against_delta_away?: number;
  finishing_delta_form_home?: number;
  finishing_delta_form_away?: number;
}

export interface ScheduleInfo {
  rest_days_home?: number;
  rest_days_away?: number;
  matches_7d_home?: number;
  matches_7d_away?: number;
  matches_14d_home?: number;
  matches_14d_away?: number;
}

export interface KpiInfo {
  status?: string;
  phase?: string;
  logloss_1x2?: number;
  brier_1x2?: number;
  roi_1x2?: number;
  brier_by_market?: Record<string, number>;
  logloss_by_market?: Record<string, number>;
  roi_by_market?: Record<string, { picks?: number; roi?: number }>;
}

export interface ScenarioAnalysis {
  base_source?: string;
  ranges?: Record<string, { min?: number; max?: number }>;
  sensitivity?: Record<string, number>;
  scenarios?: ScenarioCase[];
}

export interface ScenarioCase {
  id: string;
  label: string;
  lambda_home?: number;
  lambda_away?: number;
  probs?: Record<string, number>;
}

export interface EdgeAnalysis {
  market: string;
  outcome: string;
  fair_prob: number;
  market_prob: number;
  edge: number;
  ev: number;
  kelly_fraction: number;
  recommended_stake: number;
  confidence_interval: [number, number];
}

export interface AuditSnapshot {
  timestamp: string;
  data_sources: DataSource[];
  model_version: string;
  parameters: Record<string, unknown>;
  tool_runs: ToolRun[];
  no_bet_reasons?: string[];
}

export interface DataSource {
  name: string;
  last_updated: string;
  coverage: number;
}

export interface ToolRun {
  tool: string;
  status: 'success' | 'error' | 'skipped';
  duration_ms: number;
  message?: string;
}

// Slate endpoint types
export interface SlateResponse {
  date: string;
  competition: string;
  matches: SlateMatch[];
  tickets: {
    easy: Ticket;
    medium: Ticket;
    hard: Ticket;
  };
}

export interface SlateMatch {
  match: Match;
  summary: string;
  top_pick: {
    market: string;
    outcome: string;
    probability: number;
    edge: number;
  };
  xg_home: number;
  xg_away: number;
  form_home: string;
  form_away: string;
}

export interface Ticket {
  name: string;
  difficulty: 'easy' | 'medium' | 'hard';
  selections: TicketSelection[];
  combined_odds: number;
  combined_probability: number;
  expected_value: number;
  suggested_stake: number;
}

export interface TicketSelection {
  match: string;
  market: string;
  outcome: string;
  odds: number;
  probability: number;
  edge: number;
}

// Odds Risk endpoint types
export interface OddsRiskRequest {
  min_edge?: number;
  max_odds?: number;
  min_probability?: number;
  markets?: string[];
  competitions?: string[];
  confidence_level?: number;
  bankroll?: number;
}

export interface OddsRiskResponse {
  opportunities: OddsOpportunity[];
  filters_applied: OddsRiskRequest;
  total_matches_scanned: number;
  timestamp: string;
}

export interface OddsOpportunity {
  match: Match;
  market: string;
  outcome: string;
  market_odds: number;
  fair_odds: number;
  probability: number;
  edge: number;
  ev: number;
  kelly_fraction: number;
  kelly_stake: number;
  fractional_kelly_stake: number;
  confidence_interval: [number, number];
  divergence_from_market: number;
  risk_rating: 'low' | 'medium' | 'high';
}

// Players endpoint types
export interface PlayersRequest {
  team?: string;
  match_id?: string;
  competition?: string;
}

export interface PlayersResponse {
  players: PlayerProjection[];
  match?: Match;
  team?: string;
}

export interface PlayerProjection {
  id: string;
  name: string;
  team: string;
  position: string;
  xg: number;
  xa: number;
  xg_share: number;
  xa_share: number;
  expected_gi: number;
  minutes_projection: number;
  form_rating: number;
  injury_status?: string;
  suspension_status?: string;
}

// Audit endpoint types
export interface AuditHistoryResponse {
  runs: AuditRun[];
  total: number;
  page: number;
  per_page: number;
}

export interface AuditRun {
  id: string;
  timestamp: string;
  match: Match;
  result: 'bet' | 'no_bet' | 'error';
  selections_made: number;
  no_bet_reasons?: string[];
  snapshot: AuditSnapshot;
}

export interface AuditDetailResponse {
  run: AuditRun;
  full_analysis: AnalyzeResponse;
}

// Dashboard KPI types
export interface DashboardKPIs {
  total_analyses_today: number;
  total_analyses_week: number;
  bets_suggested: number;
  avg_edge: number;
  avg_ev: number;
  model_accuracy: number;
  roi_last_30_days: number;
  competitions_covered: string[];
  last_updated: string;
}

// Chat types
export interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
}

export interface ChatRequest {
  message: string;
  session_id?: string;
}

export interface ChatResponse {
  message: string;
  session_id: string;
  context?: Record<string, unknown>;
}

// API Error type
export interface ApiError {
  error: string;
  message: string;
  status: number;
}
