'use client';

import { cn } from '@/lib/utils';
import type { Scoreline } from '@/lib/types';

interface ScorelineTableProps {
  data: Scoreline[];
  homeTeam?: string;
  awayTeam?: string;
  className?: string;
}

export function ScorelineTable({
  data,
  homeTeam = 'Casa',
  awayTeam = 'Trasferta',
  className,
}: ScorelineTableProps) {
  const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`;

  // Get max probability for color scaling
  const maxProb = Math.max(...data.map((s) => s.probability));

  const getBackgroundOpacity = (prob: number) => {
    const ratio = prob / maxProb;
    return ratio * 0.3;
  };

  return (
    <div className={cn('rounded-lg border border-border bg-card', className)}>
      <div className="border-b border-border px-4 py-3">
        <h3 className="text-sm font-medium">Top Risultati Esatti</h3>
        <p className="text-xs text-muted-foreground mt-1">
          {homeTeam} vs {awayTeam}
        </p>
      </div>
      <div className="p-4">
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5">
          {data.map((scoreline) => {
            const isHomeWin = scoreline.home_goals > scoreline.away_goals;
            const isDraw = scoreline.home_goals === scoreline.away_goals;
            const isAwayWin = scoreline.home_goals < scoreline.away_goals;

            return (
              <div
                key={`${scoreline.home_goals}-${scoreline.away_goals}`}
                className={cn(
                  'relative rounded-md border border-border p-3 text-center transition-colors',
                  'hover:border-primary/50'
                )}
                style={{
                  backgroundColor: `oklch(0.65 0.2 ${
                    isHomeWin ? 145 : isDraw ? 65 : 250
                  } / ${getBackgroundOpacity(scoreline.probability)})`,
                }}
              >
                <div className="text-lg font-bold font-mono">
                  {scoreline.home_goals} - {scoreline.away_goals}
                </div>
                <div className="text-xs text-muted-foreground font-mono tabular-nums">
                  {formatPercent(scoreline.probability)}
                </div>
                <div
                  className={cn(
                    'absolute top-1 right-1 w-2 h-2 rounded-full',
                    isHomeWin && 'bg-success',
                    isDraw && 'bg-warning',
                    isAwayWin && 'bg-info'
                  )}
                />
              </div>
            );
          })}
        </div>
        <div className="flex items-center justify-center gap-6 mt-4 text-xs text-muted-foreground">
          <div className="flex items-center gap-1.5">
            <div className="w-2 h-2 rounded-full bg-success" />
            <span>{homeTeam}</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-2 h-2 rounded-full bg-warning" />
            <span>Pareggio</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-2 h-2 rounded-full bg-info" />
            <span>{awayTeam}</span>
          </div>
        </div>
      </div>
    </div>
  );
}
