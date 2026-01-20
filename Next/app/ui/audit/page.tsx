'use client';

import { useState } from 'react';
import useSWR from 'swr';
import { getAuditHistory, getAuditDetail } from '@/lib/api';
import type { AuditHistoryResponse, AuditRun, AuditDetailResponse } from '@/lib/types';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { LoadingState, ErrorState, EmptyState } from '@/components/states';
import { AuditSnapshotCard } from '@/components/audit-snapshot-card';
import { cn } from '@/lib/utils';
import {
  FileText,
  RefreshCw,
  ChevronLeft,
  ChevronRight,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Eye,
} from 'lucide-react';

export default function AuditPage() {
  const [page, setPage] = useState(1);
  const [selectedRun, setSelectedRun] = useState<string | null>(null);
  const perPage = 20;

  const { data, error, isLoading, mutate } = useSWR<AuditHistoryResponse>(
    ['audit-history', page, perPage],
    () => getAuditHistory(page, perPage),
    { revalidateOnFocus: false }
  );

  const totalPages = data ? Math.ceil(data.total / perPage) : 1;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Storico Audit</h1>
          <p className="text-muted-foreground">
            Cronologia analisi con snapshot dati, tool runs e no-bet reasons
          </p>
        </div>
        <Button variant="outline" onClick={() => mutate()} disabled={isLoading}>
          <RefreshCw className={cn('h-4 w-4 mr-2', isLoading && 'animate-spin')} />
          Aggiorna
        </Button>
      </div>

      {/* Loading/Error States */}
      {isLoading && <LoadingState message="Caricamento storico..." />}

      {error && (
        <ErrorState
          title="Errore"
          message={error.message}
          onRetry={() => mutate()}
        />
      )}

      {/* Results */}
      {!isLoading && !error && data && (
        <div className="space-y-4">
          {/* Summary */}
          <div className="flex items-center justify-between text-sm text-muted-foreground">
            <span>{data.total} analisi totali</span>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setPage((p) => Math.max(1, p - 1))}
                disabled={page === 1}
              >
                <ChevronLeft className="h-4 w-4" />
              </Button>
              <span>
                Pagina {page} di {totalPages}
              </span>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                disabled={page === totalPages}
              >
                <ChevronRight className="h-4 w-4" />
              </Button>
            </div>
          </div>

          {/* Audit List */}
          {data.runs.length === 0 ? (
            <EmptyState
              title="Nessuna analisi"
              message="Non ci sono ancora analisi nello storico"
            />
          ) : (
            <div className="space-y-3">
              {data.runs.map((run) => (
                <AuditRunCard
                  key={run.id}
                  run={run}
                  onViewDetails={() => setSelectedRun(run.id)}
                />
              ))}
            </div>
          )}
        </div>
      )}

      {/* Detail Dialog */}
      <AuditDetailDialog
        runId={selectedRun}
        open={!!selectedRun}
        onClose={() => setSelectedRun(null)}
      />
    </div>
  );
}

function AuditRunCard({
  run,
  onViewDetails,
}: {
  run: AuditRun;
  onViewDetails: () => void;
}) {
  const resultConfig = {
    bet: {
      label: 'Bet',
      icon: CheckCircle,
      color: 'bg-success/10 text-success border-success/20',
    },
    no_bet: {
      label: 'No Bet',
      icon: XCircle,
      color: 'bg-warning/10 text-warning border-warning/20',
    },
    error: {
      label: 'Errore',
      icon: AlertTriangle,
      color: 'bg-destructive/10 text-destructive border-destructive/20',
    },
  };

  const config = resultConfig[run.result];
  const ResultIcon = config.icon;

  const formatDate = (date: string) =>
    new Date(date).toLocaleString('it-IT', {
      dateStyle: 'short',
      timeStyle: 'short',
    });

  return (
    <Card className="bg-card border-border hover:border-primary/50 transition-colors">
      <CardContent className="py-4">
        <div className="flex flex-col md:flex-row md:items-center gap-4">
          {/* Result Badge */}
          <div className="flex items-center gap-3">
            <div className={cn('rounded-full p-2', config.color.split(' ')[0])}>
              <ResultIcon className={cn('h-4 w-4', config.color.split(' ')[1])} />
            </div>
            <Badge variant="outline" className={cn('text-xs', config.color)}>
              {config.label}
            </Badge>
          </div>

          {/* Match Info */}
          <div className="flex-1">
            <p className="font-semibold">
              {run.match.home_team} vs {run.match.away_team}
            </p>
            <p className="text-sm text-muted-foreground">
              {run.match.competition} - {run.match.date}
            </p>
          </div>

          {/* Stats */}
          <div className="flex items-center gap-6 text-sm">
            <div className="text-center">
              <p className="text-muted-foreground">Selezioni</p>
              <p className="font-mono font-bold">{run.selections_made}</p>
            </div>
            <div className="text-center">
              <p className="text-muted-foreground">Timestamp</p>
              <p className="font-mono text-xs">{formatDate(run.timestamp)}</p>
            </div>
          </div>

          {/* View Details */}
          <Button variant="outline" size="sm" onClick={onViewDetails}>
            <Eye className="h-4 w-4 mr-2" />
            Dettagli
          </Button>
        </div>

        {/* No Bet Reasons */}
        {run.no_bet_reasons && run.no_bet_reasons.length > 0 && (
          <div className="mt-3 pt-3 border-t border-border">
            <p className="text-xs text-muted-foreground mb-1">Motivi No-Bet:</p>
            <div className="flex flex-wrap gap-1">
              {run.no_bet_reasons.map((reason, idx) => (
                <Badge key={idx} variant="secondary" className="text-xs">
                  {reason}
                </Badge>
              ))}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function AuditDetailDialog({
  runId,
  open,
  onClose,
}: {
  runId: string | null;
  open: boolean;
  onClose: () => void;
}) {
  const { data, error, isLoading } = useSWR<AuditDetailResponse>(
    runId ? ['audit-detail', runId] : null,
    () => (runId ? getAuditDetail(runId) : Promise.reject('No run ID')),
    { revalidateOnFocus: false }
  );

  return (
    <Dialog open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <FileText className="h-5 w-5" />
            Dettaglio Audit
          </DialogTitle>
        </DialogHeader>

        {isLoading && <LoadingState message="Caricamento dettagli..." />}

        {error && (
          <ErrorState title="Errore" message="Impossibile caricare i dettagli" />
        )}

        {!isLoading && !error && data && (
          <div className="space-y-6">
            {/* Match Info */}
            <Card className="bg-secondary/30 border-border">
              <CardContent className="py-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="font-bold text-lg">
                      {data.run.match.home_team} vs {data.run.match.away_team}
                    </p>
                    <p className="text-sm text-muted-foreground">
                      {data.run.match.competition} - {data.run.match.date}
                    </p>
                  </div>
                  <div className="text-right">
                    <p className="text-sm text-muted-foreground">Selezioni</p>
                    <p className="font-mono text-2xl font-bold text-primary">
                      {data.run.selections_made}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Audit Snapshot */}
            <AuditSnapshotCard snapshot={data.run.snapshot} />
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
