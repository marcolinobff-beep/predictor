'use client';

import React from "react"
import { useSearchParams } from 'next/navigation';
import { Suspense } from 'react';
import Loading from './loading';

import { useState } from 'react';
import { analyzeMatch } from '@/lib/api';
import type { AnalyzeResponse, AnalyzeRequest } from '@/lib/types';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { ProbabilityTable } from '@/components/probability-table';
import { ScorelineTable } from '@/components/scoreline-table';
import { LoadingState, ErrorState, EmptyState } from '@/components/states';
import { EdgeAnalysisTable } from '@/components/edge-analysis-table';
import { AuditSnapshotCard } from '@/components/audit-snapshot-card';
import { Search, Activity } from 'lucide-react';

export default function AnalyzePage() {
  const searchParams = useSearchParams();
  const [homeTeam, setHomeTeam] = useState(searchParams.get('homeTeam') || '');
  const [awayTeam, setAwayTeam] = useState(searchParams.get('awayTeam') || '');
  const [competition, setCompetition] = useState(searchParams.get('competition') || '');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<AnalyzeResponse | null>(null);

  const handleAnalyze = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    if (!homeTeam.trim() || !awayTeam.trim()) {
      setError('Inserisci entrambe le squadre');
      return;
    }

    setIsLoading(true);

    try {
      const request: AnalyzeRequest = {
        home_team: homeTeam.trim(),
        away_team: awayTeam.trim(),
        competition: competition.trim() || undefined,
      };

      const data = await analyzeMatch(request);
      setResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Errore durante l\'analisi');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Suspense fallback={<Loading />}>
      <div className="space-y-6">
        {/* Header */}
        <div>
          <h1 className="text-2xl font-bold">Analisi Match</h1>
          <p className="text-muted-foreground">
            Simulazione Poisson + Dixon-Coles con output 1X2, OU2.5, BTTS, fair odds e scoreline
          </p>
        </div>

        {/* Search Form */}
        <Card className="bg-card border-border">
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Search className="h-4 w-4" />
              Seleziona Match
            </CardTitle>
          </CardHeader>
          <CardContent>
            <form onSubmit={handleAnalyze} className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="homeTeam">Squadra Casa</Label>
                  <Input
                    id="homeTeam"
                    value={homeTeam}
                    onChange={(e) => setHomeTeam(e.target.value)}
                    placeholder="Es. Juventus"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="awayTeam">Squadra Trasferta</Label>
                  <Input
                    id="awayTeam"
                    value={awayTeam}
                    onChange={(e) => setAwayTeam(e.target.value)}
                    placeholder="Es. Inter"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="competition">Competizione (opzionale)</Label>
                  <Input
                    id="competition"
                    value={competition}
                    onChange={(e) => setCompetition(e.target.value)}
                    placeholder="Es. Serie A"
                  />
                </div>
              </div>

              {error && <p className="text-sm text-destructive">{error}</p>}

              <Button type="submit" disabled={isLoading}>
                {isLoading ? (
                  <>
                    <Activity className="h-4 w-4 mr-2 animate-spin" />
                    Analisi in corso...
                  </>
                ) : (
                  <>
                    <Search className="h-4 w-4 mr-2" />
                    Analizza Match
                  </>
                )}
              </Button>
            </form>
          </CardContent>
        </Card>

        {/* Results */}
        {isLoading && <LoadingState message="Esecuzione simulazione..." />}

        {!isLoading && !result && !error && (
          <EmptyState
            title="Nessuna analisi"
            message="Inserisci le squadre e avvia l'analisi per vedere i risultati"
          />
        )}

        {!isLoading && error && !result && (
          <ErrorState title="Errore" message={error} />
        )}

        {!isLoading && result && (
          <div className="space-y-6">
            {/* Match Header */}
            <Card className="bg-card border-border">
              <CardContent className="py-6">
                <div className="flex items-center justify-center gap-4">
                  <div className="text-right">
                    <p className="text-xl font-bold">{result.match.home_team}</p>
                    <p className="text-sm text-muted-foreground">Casa</p>
                  </div>
                  <div className="px-4 py-2 rounded-md bg-secondary text-2xl font-mono font-bold">
                    VS
                  </div>
                  <div className="text-left">
                    <p className="text-xl font-bold">{result.match.away_team}</p>
                    <p className="text-sm text-muted-foreground">Trasferta</p>
                  </div>
                </div>
                {result.match.competition && (
                  <p className="text-center text-sm text-muted-foreground mt-3">
                    {result.match.competition} - {result.match.date}
                  </p>
                )}
              </CardContent>
            </Card>

            {/* Tabs */}
            <Tabs defaultValue="probabilities" className="space-y-4">
              <TabsList className="grid grid-cols-6 w-full">
                <TabsTrigger value="probabilities">Probabilita</TabsTrigger>
                <TabsTrigger value="fair-odds">Fair Odds</TabsTrigger>
                <TabsTrigger value="edge">Edge/EV</TabsTrigger>
                <TabsTrigger value="scorelines">Risultati</TabsTrigger>
                <TabsTrigger value="pro">Output Pro</TabsTrigger>
                <TabsTrigger value="audit">Audit</TabsTrigger>
              </TabsList>

              <TabsContent value="probabilities" className="space-y-4">
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                  <ProbabilityTable
                    data={result.probabilities.result_1x2}
                    title="Esito Finale (1X2)"
                    showEdge={false}
                  />
                  <ProbabilityTable
                    data={result.probabilities.over_under}
                    title="Over/Under 2.5"
                    showEdge={false}
                  />
                  <ProbabilityTable
                    data={result.probabilities.btts}
                    title="BTTS (Entrambe Segnano)"
                    showEdge={false}
                  />
                </div>
              </TabsContent>

              <TabsContent value="fair-odds" className="space-y-4">
                <FairOddsDisplay fairOdds={result.fair_odds} />
              </TabsContent>

              <TabsContent value="edge" className="space-y-4">
                <EdgeAnalysisTable data={result.edge_analysis} />
              </TabsContent>

              <TabsContent value="scorelines" className="space-y-4">
                <ScorelineTable
                  data={result.top_scorelines}
                  homeTeam={result.match.home_team}
                  awayTeam={result.match.away_team}
                />
              </TabsContent>

              <TabsContent value="pro" className="space-y-4">
                <ProAnalysisPanel analysis={result.analysis_pro} />
              </TabsContent>

              <TabsContent value="audit" className="space-y-4">
                <AuditSnapshotCard snapshot={result.audit_snapshot} />
              </TabsContent>
            </Tabs>
          </div>
        )}
      </div>
    </Suspense>
  );
}

function FairOddsDisplay({ fairOdds }: { fairOdds: AnalyzeResponse['fair_odds'] }) {
  const formatOdds = (value: number) => value.toFixed(2);
  const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`;

  const markets = [
    { label: '1 (Casa)', odds: fairOdds.home, prob: fairOdds.implied_prob_home },
    { label: 'X (Pareggio)', odds: fairOdds.draw, prob: fairOdds.implied_prob_draw },
    { label: '2 (Trasferta)', odds: fairOdds.away, prob: fairOdds.implied_prob_away },
    { label: 'Over 2.5', odds: fairOdds.over_2_5 },
    { label: 'Under 2.5', odds: fairOdds.under_2_5 },
    { label: 'BTTS Si', odds: fairOdds.btts_yes },
    { label: 'BTTS No', odds: fairOdds.btts_no },
  ];

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-3">
      {markets.map((market) => (
        <Card key={market.label} className="bg-card border-border">
          <CardContent className="p-4 text-center">
            <p className="text-xs text-muted-foreground mb-1">{market.label}</p>
            <p className="text-2xl font-bold font-mono tabular-nums text-primary">
              {formatOdds(market.odds)}
            </p>
            {market.prob !== undefined && (
              <p className="text-xs text-muted-foreground mt-1">
                {formatPercent(market.prob)}
              </p>
            )}
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function ProAnalysisPanel({ analysis }: { analysis?: AnalyzeResponse['analysis_pro'] }) {
  if (!analysis) {
    return (
      <Card className="bg-card border-border">
        <CardContent className="p-6 text-sm text-muted-foreground">
          Nessun output pro disponibile per questo match.
        </CardContent>
      </Card>
    );
  }

  type AnalysisPro = NonNullable<AnalyzeResponse['analysis_pro']>;
  const confidence: NonNullable<AnalysisPro['confidence']> = analysis.confidence ?? {};
  const drivers: NonNullable<AnalysisPro['drivers']> = analysis.drivers ?? [];
  const tactical: NonNullable<AnalysisPro['tactical']> = analysis.tactical ?? {};
  const lineup: NonNullable<AnalysisPro['lineup']> = analysis.lineup ?? {};
  const form: NonNullable<AnalysisPro['form']> = analysis.form ?? {};
  const schedule: NonNullable<AnalysisPro['schedule']> = analysis.schedule ?? {};
  const kpi: NonNullable<AnalysisPro['kpi']> = analysis.kpi ?? {};
  const scenario: NonNullable<AnalysisPro['scenario']> = analysis.scenario ?? {};

  const formatPct = (value?: number) =>
    value === undefined || value === null ? '-' : `${(value * 100).toFixed(1)}%`;
  const formatNum = (value?: number, digits = 2) =>
    value === undefined || value === null ? '-' : value.toFixed(digits);
  const formatSignedPct = (value?: number) =>
    value === undefined || value === null
      ? '-'
      : `${value >= 0 ? '+' : ''}${(value * 100).toFixed(1)}%`;
  const formatSigned = (value?: number, digits = 2) =>
    value === undefined || value === null
      ? '-'
      : `${value >= 0 ? '+' : ''}${value.toFixed(digits)}`;
  const formatDriverDelta = (pct?: number, raw?: number) => {
    if (pct !== undefined && pct !== null) {
      return formatSignedPct(pct);
    }
    if (raw !== undefined && raw !== null) {
      return formatSigned(raw);
    }
    return '-';
  };

  const rangeLabels: Record<string, string> = {
    home_win: '1',
    draw: 'X',
    away_win: '2',
    over_2_5: 'Over 2.5',
    under_2_5: 'Under 2.5',
    btts_yes: 'BTTS Si',
    btts_no: 'BTTS No',
  };

  const scenarioRanges = scenario.ranges || {};
  const scenarioSensitivity = scenario.sensitivity || {};
  const orderedRangeKeys = [
    'home_win',
    'draw',
    'away_win',
    'under_2_5',
    'over_2_5',
    'btts_no',
    'btts_yes',
  ];

  const scenarioRows = orderedRangeKeys
    .filter((key) => scenarioRanges[key])
    .map((key) => {
      const range = scenarioRanges[key] || {};
      const min = range.min;
      const max = range.max;
      let rangeLabel = '-';
      if (min !== undefined && max !== undefined) {
        rangeLabel =
          Math.abs(min - max) < 0.001
            ? `${(min * 100).toFixed(1)}%`
            : `${(min * 100).toFixed(1)}-${(max * 100).toFixed(1)}%`;
      }
      const sens = scenarioSensitivity[key];
      const sensLabel =
        sens === undefined || sens === null ? '-' : `${(sens * 100).toFixed(1)} pp`;
      return {
        key,
        label: rangeLabels[key] || key,
        range: rangeLabel,
        sensitivity: sensLabel,
      };
    });

  return (
    <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
      <Card className="bg-card border-border">
        <CardHeader>
          <CardTitle className="text-base">Affidabilita Modello</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          <div className="flex items-center justify-between">
            <span>Score</span>
            <span className="font-medium">{formatPct(confidence.score)}</span>
          </div>
          <div className="flex items-center justify-between">
            <span>Lineup coverage</span>
            <span className="font-medium">{formatPct(confidence.lineup_coverage)}</span>
          </div>
          <div className="flex items-center justify-between">
            <span>Stabilita forma</span>
            <span className="font-medium">{formatPct(confidence.form_stability)}</span>
          </div>
          <div className="flex items-center justify-between">
            <span>Qualita dati</span>
            <span className="font-medium">{formatPct(confidence.data_quality_score)}</span>
          </div>
        </CardContent>
      </Card>

      <Card className="bg-card border-border">
        <CardHeader>
          <CardTitle className="text-base">Motivi Principali</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          {drivers.length ? (
            drivers.map((driver, idx) => (
              <div
                key={`${driver.key ?? 'driver'}-${idx}`}
                className="rounded-md border border-border/60 px-3 py-2"
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="font-medium">
                    {driver.label || driver.key || 'Driver'}
                  </span>
                  <span className="text-xs text-muted-foreground">
                    Casa {formatDriverDelta(driver.home_delta_pct, driver.home_delta)} | Trasferta{' '}
                    {formatDriverDelta(driver.away_delta_pct, driver.away_delta)}
                  </span>
                </div>
                {driver.note && (
                  <p className="text-xs text-muted-foreground mt-1">{driver.note}</p>
                )}
              </div>
            ))
          ) : (
            <p className="text-sm text-muted-foreground">Nessun driver disponibile.</p>
          )}
        </CardContent>
      </Card>

      <Card className="bg-card border-border">
        <CardHeader>
          <CardTitle className="text-base">Match-up Tattico</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          <div className="flex items-center justify-between">
            <span>Tempo</span>
            <span className="font-medium">{tactical.tempo || '-'}</span>
          </div>
          <div className="flex items-center justify-between">
            <span>Matchup</span>
            <span className="font-medium">{tactical.matchup || '-'}</span>
          </div>
          <div className="flex items-center justify-between">
            <span>Fonte</span>
            <span className="font-medium">{tactical.source || '-'}</span>
          </div>
          <div className="flex flex-wrap gap-2 pt-2">
            {(tactical.tags || []).length ? (
              tactical.tags?.map((tag) => (
                <Badge key={tag} variant="secondary" className="text-xs">
                  {tag}
                </Badge>
              ))
            ) : (
              <span className="text-xs text-muted-foreground">Nessun tag tattico.</span>
            )}
          </div>
          {tactical.style_matchup && (
            <div className="pt-2 text-xs text-muted-foreground">
              {tactical.style_matchup.indicator || 'neutral'}{' '}
              {tactical.style_matchup.reason ? `- ${tactical.style_matchup.reason}` : ''}
            </div>
          )}
        </CardContent>
      </Card>

      <Card className="bg-card border-border">
        <CardHeader>
          <CardTitle className="text-base">Scenario Analysis</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span>Mercato</span>
            <span>Range / Sensibilita</span>
          </div>
          {scenarioRows.length ? (
            scenarioRows.map((row) => (
              <div key={row.key} className="flex items-center justify-between">
                <span className="font-medium">{row.label}</span>
                <span className="text-xs text-muted-foreground">
                  {row.range} | {row.sensitivity}
                </span>
              </div>
            ))
          ) : (
            <p className="text-sm text-muted-foreground">Nessun range scenario disponibile.</p>
          )}
          <div className="pt-2 text-xs text-muted-foreground">
            Base source: {scenario.base_source || 'n/a'}
          </div>
        </CardContent>
      </Card>

      <Card className="bg-card border-border">
        <CardHeader>
          <CardTitle className="text-base">Formazioni & Assenze</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          <div className="flex items-center justify-between">
            <span>Fonte</span>
            <span className="font-medium">{lineup.lineup_source || '-'}</span>
          </div>
          <div className="flex items-center justify-between">
            <span>Confidence</span>
            <span className="font-medium">{formatPct(lineup.lineup_confidence)}</span>
          </div>
          <div className="flex items-center justify-between">
            <span>Coverage (casa/trasferta)</span>
            <span className="font-medium">
              {formatPct(lineup.coverage_home)} / {formatPct(lineup.coverage_away)}
            </span>
          </div>
          <div className="flex items-center justify-between">
            <span>Assenze share (casa/trasferta)</span>
            <span className="font-medium">
              {formatPct(lineup.absence_share_home)} / {formatPct(lineup.absence_share_away)}
            </span>
          </div>
          <div className="flex items-center justify-between">
            <span>Penalty (casa/trasferta)</span>
            <span className="font-medium">
              {formatPct(lineup.penalty_home)} / {formatPct(lineup.penalty_away)}
            </span>
          </div>
        </CardContent>
      </Card>

      <Card className="bg-card border-border">
        <CardHeader>
          <CardTitle className="text-base">Forma & Calendario</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          <div className="flex items-center justify-between">
            <span>xG delta (casa/trasferta)</span>
            <span className="font-medium">
              {formatSignedPct(form.xg_for_delta_home)} / {formatSignedPct(form.xg_for_delta_away)}
            </span>
          </div>
          <div className="flex items-center justify-between">
            <span>xG against delta (casa/trasferta)</span>
            <span className="font-medium">
              {formatSignedPct(form.xg_against_delta_home)} / {formatSignedPct(form.xg_against_delta_away)}
            </span>
          </div>
          <div className="flex items-center justify-between">
            <span>Finishing delta (casa/trasferta)</span>
            <span className="font-medium">
              {formatSignedPct(form.finishing_delta_form_home)} / {formatSignedPct(form.finishing_delta_form_away)}
            </span>
          </div>
          <div className="pt-2 grid grid-cols-2 gap-2 text-xs text-muted-foreground">
            <div>Rest days: {formatNum(schedule.rest_days_home, 1)} / {formatNum(schedule.rest_days_away, 1)}</div>
            <div>Last 7d: {formatNum(schedule.matches_7d_home, 0)} / {formatNum(schedule.matches_7d_away, 0)}</div>
            <div>Last 14d: {formatNum(schedule.matches_14d_home, 0)} / {formatNum(schedule.matches_14d_away, 0)}</div>
          </div>
        </CardContent>
      </Card>

      <Card className="bg-card border-border xl:col-span-2">
        <CardHeader>
          <CardTitle className="text-base">KPI Modello</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 text-sm">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div className="rounded-md border border-border/60 px-3 py-2">
              <p className="text-xs text-muted-foreground">LogLoss 1X2</p>
              <p className="text-lg font-semibold">{formatNum(kpi.logloss_1x2, 3)}</p>
            </div>
            <div className="rounded-md border border-border/60 px-3 py-2">
              <p className="text-xs text-muted-foreground">Brier 1X2</p>
              <p className="text-lg font-semibold">{formatNum(kpi.brier_1x2, 3)}</p>
            </div>
            <div className="rounded-md border border-border/60 px-3 py-2">
              <p className="text-xs text-muted-foreground">ROI 1X2</p>
              <p className="text-lg font-semibold">{formatNum(kpi.roi_1x2, 3)}</p>
            </div>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-xs text-muted-foreground">
            {kpi.brier_by_market && Object.keys(kpi.brier_by_market).length ? (
              Object.entries(kpi.brier_by_market).map(([market, value]) => (
                <div key={`brier-${market}`} className="rounded-md border border-border/60 px-3 py-2">
                  <p className="text-xs text-muted-foreground">Brier {market}</p>
                  <p className="text-sm font-medium text-foreground">{formatNum(value, 3)}</p>
                </div>
              ))
            ) : (
              <div className="rounded-md border border-border/60 px-3 py-2">
                <p className="text-xs text-muted-foreground">Brier per mercato</p>
                <p className="text-sm">-</p>
              </div>
            )}
            {kpi.logloss_by_market && Object.keys(kpi.logloss_by_market).length ? (
              Object.entries(kpi.logloss_by_market).map(([market, value]) => (
                <div key={`logloss-${market}`} className="rounded-md border border-border/60 px-3 py-2">
                  <p className="text-xs text-muted-foreground">LogLoss {market}</p>
                  <p className="text-sm font-medium text-foreground">{formatNum(value, 3)}</p>
                </div>
              ))
            ) : (
              <div className="rounded-md border border-border/60 px-3 py-2">
                <p className="text-xs text-muted-foreground">LogLoss per mercato</p>
                <p className="text-sm">-</p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
