'use client';

import useSWR from 'swr';
import { getDashboardKPIs } from '@/lib/api';
import { KpiCard } from '@/components/kpi-card';
import { LoadingState, ErrorState } from '@/components/states';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Activity,
  BarChart3,
  Calculator,
  Target,
  TrendingUp,
  Trophy,
  Percent,
  Globe,
} from 'lucide-react';

export default function DashboardPage() {
  const { data, error, isLoading, mutate } = useSWR('dashboard-kpis', getDashboardKPIs);

  if (isLoading) {
    return <LoadingState message="Caricamento dashboard..." />;
  }

  if (error) {
    return (
      <ErrorState
        title="Errore caricamento"
        message={error.message}
        onRetry={() => mutate()}
      />
    );
  }

  const kpis = data;

  // Format helpers
  const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Dashboard</h1>
          <p className="text-muted-foreground">
            Panoramica delle performance e metriche chiave
          </p>
        </div>
        {kpis?.last_updated && (
          <p className="text-xs text-muted-foreground">
            Ultimo aggiornamento: {new Date(kpis.last_updated).toLocaleString('it-IT')}
          </p>
        )}
      </div>

      {/* KPI Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          title="Analisi Oggi"
          value={kpis?.total_analyses_today ?? 0}
          subtitle="Match analizzati"
          icon={Activity}
        />
        <KpiCard
          title="Analisi Settimana"
          value={kpis?.total_analyses_week ?? 0}
          subtitle="Ultimi 7 giorni"
          icon={BarChart3}
        />
        <KpiCard
          title="Bet Suggerite"
          value={kpis?.bets_suggested ?? 0}
          subtitle="Con edge positivo"
          icon={Target}
        />
        <KpiCard
          title="Accuratezza Modello"
          value={kpis ? formatPercent(kpis.model_accuracy) : '-'}
          subtitle="Previsioni corrette"
          icon={Trophy}
        />
      </div>

      {/* Secondary KPIs */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <KpiCard
          title="Edge Medio"
          value={kpis ? formatPercent(kpis.avg_edge) : '-'}
          subtitle="Su scommesse suggerite"
          icon={Calculator}
          trend={
            kpis?.avg_edge
              ? {
                  value: kpis.avg_edge * 100,
                  label: '',
                  positive: kpis.avg_edge > 0,
                }
              : undefined
          }
        />
        <KpiCard
          title="EV Medio"
          value={kpis ? formatPercent(kpis.avg_ev) : '-'}
          subtitle="Expected Value"
          icon={Percent}
          trend={
            kpis?.avg_ev
              ? {
                  value: kpis.avg_ev * 100,
                  label: '',
                  positive: kpis.avg_ev > 0,
                }
              : undefined
          }
        />
        <KpiCard
          title="ROI 30 Giorni"
          value={kpis ? formatPercent(kpis.roi_last_30_days) : '-'}
          subtitle="Return on Investment"
          icon={TrendingUp}
          trend={
            kpis?.roi_last_30_days
              ? {
                  value: kpis.roi_last_30_days * 100,
                  label: 'vs periodo precedente',
                  positive: kpis.roi_last_30_days > 0,
                }
              : undefined
          }
        />
      </div>

      {/* Competitions Covered */}
      <Card className="bg-card border-border">
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-base">
            <Globe className="h-4 w-4" />
            Competizioni Coperte
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-2">
            {kpis?.competitions_covered?.length ? (
              kpis.competitions_covered.map((comp) => (
                <Badge key={comp} variant="secondary" className="text-sm">
                  {comp}
                </Badge>
              ))
            ) : (
              <p className="text-sm text-muted-foreground">
                Nessuna competizione configurata
              </p>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Quick Actions */}
      <Card className="bg-card border-border">
        <CardHeader>
          <CardTitle className="text-base">Azioni Rapide</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <QuickActionLink href="/ui/analyze" label="Analizza Match" icon={Activity} />
            <QuickActionLink href="/ui/slate" label="Pronostici Oggi" icon={Target} />
            <QuickActionLink href="/ui/odds-risk" label="Screener Quote" icon={TrendingUp} />
            <QuickActionLink href="/ui/audit" label="Storico Audit" icon={BarChart3} />
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function QuickActionLink({
  href,
  label,
  icon: Icon,
}: {
  href: string;
  label: string;
  icon: typeof Activity;
}) {
  return (
    <a
      href={href}
      className="flex flex-col items-center justify-center gap-2 rounded-lg border border-border bg-secondary/50 p-4 text-center transition-colors hover:bg-secondary hover:border-primary/50"
    >
      <Icon className="h-5 w-5 text-muted-foreground" />
      <span className="text-sm font-medium">{label}</span>
    </a>
  );
}
