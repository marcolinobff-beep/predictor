'use client';

import { useState } from 'react';
import useSWR from 'swr';
import { getPlayerProjections } from '@/lib/api';
import type { PlayersResponse, PlayerProjection, PlayersRequest } from '@/lib/types';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { LoadingState, ErrorState, EmptyState } from '@/components/states';
import { cn } from '@/lib/utils';
import { Users, Search, RefreshCw, AlertTriangle, Ban } from 'lucide-react';
import { useSearchParams } from 'next/navigation';
import Loading from './loading';

export default function PlayersPage() {
  const [team, setTeam] = useState('');
  const [competition, setCompetition] = useState('');
  const [appliedFilters, setAppliedFilters] = useState<PlayersRequest>({});
  const searchParams = useSearchParams();

  const { data, error, isLoading, mutate } = useSWR<PlayersResponse>(
    ['players', JSON.stringify(appliedFilters)],
    () => getPlayerProjections(appliedFilters),
    { revalidateOnFocus: false }
  );

  const handleSearch = () => {
    setAppliedFilters({
      team: team.trim() || undefined,
      competition: competition.trim() || undefined,
    });
  };

  const handleReset = () => {
    setTeam('');
    setCompetition('');
    setAppliedFilters({});
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Proiezioni Giocatori</h1>
          <p className="text-muted-foreground">
            Metriche per player: xG, xA, share, expected GI per team e match
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
              <Label htmlFor="team">Squadra</Label>
              <Input
                id="team"
                value={team}
                onChange={(e) => setTeam(e.target.value)}
                placeholder="Es. Juventus"
              />
            </div>
            <div className="flex-1 space-y-2">
              <Label htmlFor="competition">Competizione</Label>
              <Input
                id="competition"
                value={competition}
                onChange={(e) => setCompetition(e.target.value)}
                placeholder="Es. Serie A"
              />
            </div>
            <div className="flex items-end gap-2">
              <Button onClick={handleSearch}>
                <Search className="h-4 w-4 mr-2" />
                Cerca
              </Button>
              <Button variant="outline" onClick={handleReset}>
                Reset
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Loading/Error States */}
      {isLoading && <LoadingState message="Caricamento proiezioni..." />}

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
          {/* Context */}
          {(data.team || data.match) && (
            <div className="flex items-center gap-4 text-sm">
              {data.team && (
                <Badge variant="secondary" className="text-sm">
                  {data.team}
                </Badge>
              )}
              {data.match && (
                <span className="text-muted-foreground">
                  {data.match.home_team} vs {data.match.away_team} - {data.match.date}
                </span>
              )}
            </div>
          )}

          {/* Players Table */}
          {data.players.length === 0 ? (
            <EmptyState
              title="Nessun giocatore"
              message="Nessuna proiezione disponibile per i filtri selezionati"
              action={{ label: 'Reset Filtri', onClick: handleReset }}
            />
          ) : (
            <PlayersTable data={data.players} />
          )}
        </div>
      )}
    </div>
  );
}

function PlayersTable({ data }: { data: PlayerProjection[] }) {
  const formatDecimal = (value: number) => value.toFixed(2);
  const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`;

  // Sort by expected_gi descending
  const sortedData = [...data].sort((a, b) => b.expected_gi - a.expected_gi);

  const getFormColor = (rating: number) => {
    if (rating >= 7.5) return 'text-success';
    if (rating >= 6.5) return 'text-warning';
    return 'text-destructive';
  };

  return (
    <Card className="bg-card border-border overflow-hidden">
      <CardHeader className="pb-3">
        <CardTitle className="text-base flex items-center gap-2">
          <Users className="h-4 w-4" />
          {data.length} Giocatori
        </CardTitle>
      </CardHeader>
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead className="text-muted-foreground">Giocatore</TableHead>
              <TableHead className="text-muted-foreground">Squadra</TableHead>
              <TableHead className="text-muted-foreground">Ruolo</TableHead>
              <TableHead className="text-right text-muted-foreground">xG</TableHead>
              <TableHead className="text-right text-muted-foreground">xA</TableHead>
              <TableHead className="text-right text-muted-foreground">xG Share</TableHead>
              <TableHead className="text-right text-muted-foreground">xA Share</TableHead>
              <TableHead className="text-right text-muted-foreground">Exp. GI</TableHead>
              <TableHead className="text-right text-muted-foreground">Minuti</TableHead>
              <TableHead className="text-right text-muted-foreground">Forma</TableHead>
              <TableHead className="text-center text-muted-foreground">Status</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {sortedData.map((player) => (
              <TableRow key={player.id} className="hover:bg-secondary/50">
                <TableCell className="font-medium">{player.name}</TableCell>
                <TableCell className="text-muted-foreground">{player.team}</TableCell>
                <TableCell>
                  <Badge variant="outline" className="text-xs">
                    {player.position}
                  </Badge>
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {formatDecimal(player.xg)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {formatDecimal(player.xa)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums text-muted-foreground">
                  {formatPercent(player.xg_share)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums text-muted-foreground">
                  {formatPercent(player.xa_share)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums font-bold text-primary">
                  {formatDecimal(player.expected_gi)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {player.minutes_projection}'
                </TableCell>
                <TableCell className={cn('text-right font-mono tabular-nums', getFormColor(player.form_rating))}>
                  {formatDecimal(player.form_rating)}
                </TableCell>
                <TableCell className="text-center">
                  <div className="flex items-center justify-center gap-1">
                    {player.injury_status && (
                      <Badge variant="outline" className="text-xs bg-destructive/10 text-destructive border-destructive/20">
                        <AlertTriangle className="h-3 w-3 mr-1" />
                        {player.injury_status}
                      </Badge>
                    )}
                    {player.suspension_status && (
                      <Badge variant="outline" className="text-xs bg-warning/10 text-warning border-warning/20">
                        <Ban className="h-3 w-3 mr-1" />
                        {player.suspension_status}
                      </Badge>
                    )}
                    {!player.injury_status && !player.suspension_status && (
                      <span className="text-xs text-muted-foreground">-</span>
                    )}
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </Card>
  );
}

// loading.tsx
// export default function Loading() {
//   return null;
// }
