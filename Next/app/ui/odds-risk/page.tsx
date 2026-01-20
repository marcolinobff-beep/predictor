'use client';

import { useState } from 'react';
import useSWR from 'swr';
import { getOddsRisk } from '@/lib/api';
import type { OddsRiskRequest, OddsRiskResponse, OddsOpportunity } from '@/lib/types';
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Slider } from '@/components/ui/slider';
import { LoadingState, ErrorState, EmptyState } from '@/components/states';
import { cn } from '@/lib/utils';
import { Filter, TrendingUp, AlertTriangle, RefreshCw, Search } from 'lucide-react';
import { useSearchParams } from 'next/navigation';
import Loading from './loading';

const MARKETS = ['1X2', 'Over/Under 2.5', 'BTTS', 'Doppia Chance', 'Handicap'];
const COMPETITIONS = ['Serie A', 'Serie B', 'Premier League', 'Bundesliga', 'La Liga', 'Ligue 1'];

export default function OddsRiskPage() {
  const searchParams = useSearchParams();
  const [filters, setFilters] = useState<OddsRiskRequest>({
    min_edge: 0.02,
    max_odds: 5.0,
    min_probability: 0.3,
    confidence_level: 0.9,
    bankroll: 1000,
  });

  const [appliedFilters, setAppliedFilters] = useState<OddsRiskRequest>(filters);

  const { data, error, isLoading, mutate } = useSWR<OddsRiskResponse>(
    ['odds-risk', JSON.stringify(appliedFilters)],
    () => getOddsRisk(appliedFilters),
    { revalidateOnFocus: false }
  );

  const handleApplyFilters = () => {
    setAppliedFilters({ ...filters });
  };

  const handleResetFilters = () => {
    const defaultFilters: OddsRiskRequest = {
      min_edge: 0.02,
      max_odds: 5.0,
      min_probability: 0.3,
      confidence_level: 0.9,
      bankroll: 1000,
    };
    setFilters(defaultFilters);
    setAppliedFilters(defaultFilters);
  };

  const formatPercent = (value: number) => `${(value * 100).toFixed(2)}%`;
  const formatOdds = (value: number) => value.toFixed(2);
  const formatCurrency = (value: number) => `€${value.toFixed(2)}`;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Quote & Rischio</h1>
          <p className="text-muted-foreground">
            Screener con filtri, edge/EV, Kelly frazionario e cap stake
          </p>
        </div>
        <Button variant="outline" onClick={() => mutate()} disabled={isLoading}>
          <RefreshCw className={cn('h-4 w-4 mr-2', isLoading && 'animate-spin')} />
          Aggiorna
        </Button>
      </div>

      {/* Filters */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Filter className="h-4 w-4" />
            Filtri Ricerca
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {/* Min Edge */}
            <div className="space-y-2">
              <Label>Edge Minimo: {formatPercent(filters.min_edge || 0)}</Label>
              <Slider
                value={[filters.min_edge ? filters.min_edge * 100 : 2]}
                onValueChange={([value]) => setFilters({ ...filters, min_edge: value / 100 })}
                min={0}
                max={20}
                step={0.5}
              />
            </div>

            {/* Max Odds */}
            <div className="space-y-2">
              <Label>Quota Massima: {formatOdds(filters.max_odds || 5)}</Label>
              <Slider
                value={[filters.max_odds || 5]}
                onValueChange={([value]) => setFilters({ ...filters, max_odds: value })}
                min={1.1}
                max={10}
                step={0.1}
              />
            </div>

            {/* Min Probability */}
            <div className="space-y-2">
              <Label>Probabilita Minima: {formatPercent(filters.min_probability || 0)}</Label>
              <Slider
                value={[filters.min_probability ? filters.min_probability * 100 : 30]}
                onValueChange={([value]) => setFilters({ ...filters, min_probability: value / 100 })}
                min={10}
                max={80}
                step={5}
              />
            </div>

            {/* Bankroll */}
            <div className="space-y-2">
              <Label htmlFor="bankroll">Bankroll</Label>
              <Input
                id="bankroll"
                type="number"
                value={filters.bankroll || 1000}
                onChange={(e) => setFilters({ ...filters, bankroll: Number(e.target.value) })}
                min={100}
                step={100}
              />
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {/* Markets */}
            <div className="space-y-2">
              <Label>Mercati</Label>
              <Select
                value={filters.markets?.join(',') || 'all'}
                onValueChange={(value) =>
                  setFilters({
                    ...filters,
                    markets: value === 'all' ? undefined : value.split(','),
                  })
                }
              >
                <SelectTrigger>
                  <SelectValue placeholder="Tutti i mercati" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Tutti i mercati</SelectItem>
                  {MARKETS.map((market) => (
                    <SelectItem key={market} value={market}>
                      {market}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Competitions */}
            <div className="space-y-2">
              <Label>Competizioni</Label>
              <Select
                value={filters.competitions?.join(',') || 'all'}
                onValueChange={(value) =>
                  setFilters({
                    ...filters,
                    competitions: value === 'all' ? undefined : value.split(','),
                  })
                }
              >
                <SelectTrigger>
                  <SelectValue placeholder="Tutte le competizioni" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Tutte le competizioni</SelectItem>
                  {COMPETITIONS.map((comp) => (
                    <SelectItem key={comp} value={comp}>
                      {comp}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Confidence Level */}
            <div className="space-y-2">
              <Label>Livello Confidenza: {formatPercent(filters.confidence_level || 0.9)}</Label>
              <Slider
                value={[filters.confidence_level ? filters.confidence_level * 100 : 90]}
                onValueChange={([value]) => setFilters({ ...filters, confidence_level: value / 100 })}
                min={80}
                max={99}
                step={1}
              />
            </div>
          </div>

          {/* Actions */}
          <div className="flex items-center gap-3">
            <Button onClick={handleApplyFilters}>
              <Search className="h-4 w-4 mr-2" />
              Applica Filtri
            </Button>
            <Button variant="outline" onClick={handleResetFilters}>
              Reset
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Loading/Error States */}
      {isLoading && <LoadingState message="Ricerca opportunita..." />}

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
          <div className="flex items-center gap-4 text-sm text-muted-foreground">
            <span>{data.opportunities.length} opportunita trovate</span>
            <span>su {data.total_matches_scanned} match analizzati</span>
            <span>
              Ultimo aggiornamento:{' '}
              {new Date(data.timestamp).toLocaleTimeString('it-IT')}
            </span>
          </div>

          {/* Results Table */}
          {data.opportunities.length === 0 ? (
            <EmptyState
              title="Nessuna opportunita"
              message="Nessuna opportunita corrisponde ai filtri selezionati. Prova ad allargare i criteri di ricerca."
              action={{ label: 'Reset Filtri', onClick: handleResetFilters }}
            />
          ) : (
            <OpportunitiesTable
              data={data.opportunities}
              bankroll={filters.bankroll || 1000}
            />
          )}
        </div>
      )}
    </div>
  );
}

function OpportunitiesTable({
  data,
  bankroll,
}: {
  data: OddsOpportunity[];
  bankroll: number;
}) {
  const formatPercent = (value: number) => `${(value * 100).toFixed(2)}%`;
  const formatOdds = (value: number) => value.toFixed(2);
  const formatCurrency = (value: number) => `€${value.toFixed(2)}`;

  const riskConfig = {
    low: { label: 'Basso', color: 'bg-success/10 text-success border-success/20' },
    medium: { label: 'Medio', color: 'bg-warning/10 text-warning border-warning/20' },
    high: { label: 'Alto', color: 'bg-destructive/10 text-destructive border-destructive/20' },
  };

  // Sort by edge descending
  const sortedData = [...data].sort((a, b) => b.edge - a.edge);

  return (
    <Card className="bg-card border-border overflow-hidden">
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead className="text-muted-foreground">Match</TableHead>
              <TableHead className="text-muted-foreground">Mercato</TableHead>
              <TableHead className="text-muted-foreground">Esito</TableHead>
              <TableHead className="text-right text-muted-foreground">Quota</TableHead>
              <TableHead className="text-right text-muted-foreground">Fair</TableHead>
              <TableHead className="text-right text-muted-foreground">Prob.</TableHead>
              <TableHead className="text-right text-muted-foreground">Edge</TableHead>
              <TableHead className="text-right text-muted-foreground">EV</TableHead>
              <TableHead className="text-right text-muted-foreground">Kelly</TableHead>
              <TableHead className="text-right text-muted-foreground">Stake (1/4)</TableHead>
              <TableHead className="text-center text-muted-foreground">Rischio</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {sortedData.map((opp, idx) => {
              const risk = riskConfig[opp.risk_rating];

              return (
                <TableRow key={idx} className="hover:bg-secondary/50">
                  <TableCell>
                    <div className="min-w-[180px]">
                      <p className="font-medium text-sm truncate">
                        {opp.match.home_team} vs {opp.match.away_team}
                      </p>
                      <p className="text-xs text-muted-foreground">
                        {opp.match.competition} - {opp.match.kickoff}
                      </p>
                    </div>
                  </TableCell>
                  <TableCell className="text-sm">{opp.market}</TableCell>
                  <TableCell className="font-medium">{opp.outcome}</TableCell>
                  <TableCell className="text-right font-mono tabular-nums">
                    {formatOdds(opp.market_odds)}
                  </TableCell>
                  <TableCell className="text-right font-mono tabular-nums text-muted-foreground">
                    {formatOdds(opp.fair_odds)}
                  </TableCell>
                  <TableCell className="text-right font-mono tabular-nums">
                    {formatPercent(opp.probability)}
                  </TableCell>
                  <TableCell
                    className={cn(
                      'text-right font-mono tabular-nums font-medium',
                      opp.edge > 0.05 ? 'text-success' : opp.edge > 0.02 ? 'text-warning' : ''
                    )}
                  >
                    +{formatPercent(opp.edge)}
                  </TableCell>
                  <TableCell
                    className={cn(
                      'text-right font-mono tabular-nums',
                      opp.ev > 0 ? 'text-success' : 'text-destructive'
                    )}
                  >
                    {opp.ev > 0 ? '+' : ''}
                    {formatPercent(opp.ev)}
                  </TableCell>
                  <TableCell className="text-right font-mono tabular-nums">
                    {formatPercent(opp.kelly_fraction)}
                  </TableCell>
                  <TableCell className="text-right font-mono tabular-nums font-bold text-primary">
                    {formatCurrency(opp.fractional_kelly_stake)}
                  </TableCell>
                  <TableCell className="text-center">
                    <Badge variant="outline" className={cn('text-xs', risk.color)}>
                      {risk.label}
                    </Badge>
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </div>
    </Card>
  );
}
