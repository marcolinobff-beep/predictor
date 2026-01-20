'use client';

import { useState } from 'react';
import useSWR from 'swr';
import { getSlate } from '@/lib/api';
import type { SlateResponse, SlateMatch } from '@/lib/types';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { TicketCard } from '@/components/ticket-card';
import { LoadingState, ErrorState, EmptyState } from '@/components/states';
import { cn } from '@/lib/utils';
import { Calendar, RefreshCw, TrendingUp, Activity } from 'lucide-react';

export default function SlatePage() {
  const [date, setDate] = useState(() => {
    const today = new Date();
    return today.toISOString().split('T')[0];
  });
  const [competition, setCompetition] = useState('');

  const { data, error, isLoading, mutate } = useSWR<SlateResponse>(
    ['slate', date, competition],
    () => getSlate(date, competition || undefined),
    { revalidateOnFocus: false }
  );

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Pronostici Giornata</h1>
          <p className="text-muted-foreground">
            Lista match con sintesi e 3 schedine (facile/media/difficile)
          </p>
        </div>
        <Button variant="outline" onClick={() => mutate()} disabled={isLoading}>
          <RefreshCw className={cn('h-4 w-4 mr-2', isLoading && 'animate-spin')} />
          Aggiorna
        </Button>
      </div>

      {/* Filters */}
      <Card className="bg-card border-border">
        <CardContent className="py-4">
          <div className="flex flex-col sm:flex-row gap-4">
            <div className="flex-1 space-y-2">
              <Label htmlFor="date">Data</Label>
              <Input
                id="date"
                type="date"
                value={date}
                onChange={(e) => setDate(e.target.value)}
              />
            </div>
            <div className="flex-1 space-y-2">
              <Label htmlFor="competition">Competizione (opzionale)</Label>
              <Input
                id="competition"
                value={competition}
                onChange={(e) => setCompetition(e.target.value)}
                placeholder="Tutte le competizioni"
              />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Loading/Error States */}
      {isLoading && <LoadingState message="Caricamento pronostici..." />}

      {error && (
        <ErrorState
          title="Errore"
          message={error.message}
          onRetry={() => mutate()}
        />
      )}

      {/* Results */}
      {!isLoading && !error && data && (
        <div className="space-y-6">
          {/* Summary Header */}
          <div className="flex items-center gap-4 text-sm text-muted-foreground">
            <div className="flex items-center gap-1">
              <Calendar className="h-4 w-4" />
              <span>{new Date(data.date).toLocaleDateString('it-IT', { dateStyle: 'full' })}</span>
            </div>
            {data.competition && (
              <Badge variant="secondary">{data.competition}</Badge>
            )}
            <span>{data.matches.length} match disponibili</span>
          </div>

          {/* Tickets */}
          {data.tickets && (
            <div>
              <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
                <TrendingUp className="h-5 w-5" />
                Schedine Suggerite
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <TicketCard ticket={data.tickets.easy} />
                <TicketCard ticket={data.tickets.medium} />
                <TicketCard ticket={data.tickets.hard} />
              </div>
            </div>
          )}

          {/* Match List */}
          <div>
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <Activity className="h-5 w-5" />
              Match del Giorno
            </h2>
            {data.matches.length === 0 ? (
              <EmptyState
                title="Nessun match"
                message="Non ci sono match programmati per questa data"
              />
            ) : (
              <div className="space-y-3">
                {data.matches.map((match) => (
                  <MatchCard key={match.match.id} data={match} />
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function MatchCard({ data }: { data: SlateMatch }) {
  const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`;

  return (
    <Card className="bg-card border-border hover:border-primary/50 transition-colors">
      <CardContent className="py-4">
        <div className="flex flex-col lg:flex-row lg:items-center gap-4">
          {/* Teams */}
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-3">
              <div className="flex-1 min-w-0">
                <p className="font-semibold truncate">{data.match.home_team}</p>
                <p className="text-sm text-muted-foreground">vs</p>
                <p className="font-semibold truncate">{data.match.away_team}</p>
              </div>
              <div className="text-right text-sm text-muted-foreground">
                <p>{data.match.competition}</p>
                <p>{data.match.kickoff}</p>
              </div>
            </div>
          </div>

          {/* xG */}
          <div className="flex items-center gap-4 text-center">
            <div>
              <p className="text-xs text-muted-foreground">xG Casa</p>
              <p className="font-mono font-bold text-lg tabular-nums">{data.xg_home.toFixed(2)}</p>
            </div>
            <div className="text-muted-foreground">-</div>
            <div>
              <p className="text-xs text-muted-foreground">xG Trasf.</p>
              <p className="font-mono font-bold text-lg tabular-nums">{data.xg_away.toFixed(2)}</p>
            </div>
          </div>

          {/* Form */}
          <div className="flex items-center gap-4">
            <FormDisplay label="Casa" form={data.form_home} />
            <FormDisplay label="Trasf." form={data.form_away} />
          </div>

          {/* Top Pick */}
          <div className="lg:ml-auto">
            <div className="rounded-md bg-primary/10 border border-primary/20 px-4 py-2">
              <p className="text-xs text-muted-foreground mb-1">Top Pick</p>
              <p className="font-medium text-sm">{data.top_pick.market}: {data.top_pick.outcome}</p>
              <div className="flex items-center gap-2 mt-1 text-xs">
                <span className="text-muted-foreground">
                  {formatPercent(data.top_pick.probability)}
                </span>
                <span className={cn(
                  'font-mono',
                  data.top_pick.edge > 0 ? 'text-success' : 'text-muted-foreground'
                )}>
                  {data.top_pick.edge > 0 ? '+' : ''}{formatPercent(data.top_pick.edge)}
                </span>
              </div>
            </div>
          </div>
        </div>

        {/* Summary */}
        <p className="text-sm text-muted-foreground mt-3 border-t border-border pt-3">
          {data.summary}
        </p>
      </CardContent>
    </Card>
  );
}

function FormDisplay({ label, form }: { label: string; form: string }) {
  const results = form.split('');

  const getResultColor = (result: string) => {
    switch (result.toUpperCase()) {
      case 'W':
        return 'bg-success text-success-foreground';
      case 'D':
        return 'bg-warning text-warning-foreground';
      case 'L':
        return 'bg-destructive text-destructive-foreground';
      default:
        return 'bg-muted text-muted-foreground';
    }
  };

  return (
    <div>
      <p className="text-xs text-muted-foreground mb-1">{label}</p>
      <div className="flex gap-0.5">
        {results.map((result, idx) => (
          <div
            key={idx}
            className={cn(
              'w-5 h-5 rounded text-xs font-bold flex items-center justify-center',
              getResultColor(result)
            )}
          >
            {result.toUpperCase()}
          </div>
        ))}
      </div>
    </div>
  );
}
