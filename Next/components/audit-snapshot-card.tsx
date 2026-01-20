'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import type { AuditSnapshot } from '@/lib/types';
import { Clock, Database, Cog, AlertTriangle, CheckCircle, XCircle } from 'lucide-react';

interface AuditSnapshotCardProps {
  snapshot: AuditSnapshot;
  className?: string;
}

export function AuditSnapshotCard({ snapshot, className }: AuditSnapshotCardProps) {
  const formatDate = (date: string) =>
    new Date(date).toLocaleString('it-IT', {
      dateStyle: 'medium',
      timeStyle: 'short',
    });

  const statusConfig = {
    success: { icon: CheckCircle, color: 'text-success', bg: 'bg-success/10' },
    error: { icon: XCircle, color: 'text-destructive', bg: 'bg-destructive/10' },
    skipped: { icon: AlertTriangle, color: 'text-warning', bg: 'bg-warning/10' },
  };

  return (
    <div className={cn('space-y-4', className)}>
      {/* Meta Info */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Clock className="h-4 w-4" />
            Informazioni Snapshot
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <p className="text-muted-foreground">Timestamp</p>
              <p className="font-medium">{formatDate(snapshot.timestamp)}</p>
            </div>
            <div>
              <p className="text-muted-foreground">Versione Modello</p>
              <p className="font-mono">{snapshot.model_version}</p>
            </div>
          </div>
          {snapshot.parameters && Object.keys(snapshot.parameters).length > 0 && (
            <div>
              <p className="text-muted-foreground text-sm mb-2">Parametri</p>
              <div className="flex flex-wrap gap-2">
                {Object.entries(snapshot.parameters).map(([key, value]) => (
                  <Badge key={key} variant="secondary" className="text-xs font-mono">
                    {key}: {String(value)}
                  </Badge>
                ))}
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Data Sources */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Database className="h-4 w-4" />
            Sorgenti Dati
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {snapshot.data_sources.map((source) => (
              <div
                key={source.name}
                className="flex items-center justify-between rounded-md bg-secondary/50 px-3 py-2"
              >
                <div>
                  <p className="font-medium text-sm">{source.name}</p>
                  <p className="text-xs text-muted-foreground">
                    Ultimo aggiornamento: {formatDate(source.last_updated)}
                  </p>
                </div>
                <div className="text-right">
                  <p className="text-sm font-mono tabular-nums">
                    {(source.coverage * 100).toFixed(0)}%
                  </p>
                  <p className="text-xs text-muted-foreground">coverage</p>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Tool Runs */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Cog className="h-4 w-4" />
            Esecuzione Tool
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {snapshot.tool_runs.map((run, idx) => {
              const config = statusConfig[run.status];
              const StatusIcon = config.icon;

              return (
                <div
                  key={idx}
                  className="flex items-center justify-between rounded-md bg-secondary/50 px-3 py-2"
                >
                  <div className="flex items-center gap-2">
                    <div className={cn('rounded-full p-1', config.bg)}>
                      <StatusIcon className={cn('h-3 w-3', config.color)} />
                    </div>
                    <div>
                      <p className="font-medium text-sm">{run.tool}</p>
                      {run.message && (
                        <p className="text-xs text-muted-foreground">{run.message}</p>
                      )}
                    </div>
                  </div>
                  <p className="text-xs font-mono tabular-nums text-muted-foreground">
                    {run.duration_ms}ms
                  </p>
                </div>
              );
            })}
          </div>
        </CardContent>
      </Card>

      {/* No Bet Reasons */}
      {snapshot.no_bet_reasons && snapshot.no_bet_reasons.length > 0 && (
        <Card className="bg-card border-border border-warning/50">
          <CardHeader className="pb-3">
            <CardTitle className="text-base flex items-center gap-2 text-warning">
              <AlertTriangle className="h-4 w-4" />
              Motivi No-Bet
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-1">
              {snapshot.no_bet_reasons.map((reason, idx) => (
                <li key={idx} className="text-sm text-muted-foreground flex items-start gap-2">
                  <span className="text-warning">•</span>
                  {reason}
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
