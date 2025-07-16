import { useState, useEffect, useCallback } from 'react';
import { Button } from './components/ui/button.jsx';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './components/ui/card.jsx';
import { Input } from './components/ui/input.jsx';
import { Label } from './components/ui/label.jsx';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './components/ui/tabs.jsx';
import { Badge } from './components/ui/badge.jsx';
import { Alert, AlertDescription, AlertTitle } from './components/ui/alert.jsx';
import { Loader2, TrendingUp, Scale, Info, Zap, Activity, LineChart, BarChart3, PieChart, Table, Search } from 'lucide-react';
import Chart from 'chart.js/auto';

import './App.css';
import './index.css';

// A URL base da API deve ser relativa para funcionar tanto em desenvolvimento quanto em produção no Render
const API_BASE_URL = '/api';

function App() {
  // ==============================================================================
  // --- SEUS ESTADOS E LÓGICA (100% PRESERVADOS DA SUA VERSÃO ORIGINAL) ---
  // ==============================================================================
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  
  // --- Estados para Valuation ---
  const [valuationReport, setValuationReport] = useState(null);
  const [valuationCompanyData, setValuationCompanyData] = useState(null);
  const [valuationTickerInput, setValuationTickerInput] = useState('');

  // --- Estados para Fleuriet ---
  const [fleurietCompanies, setFleurietCompanies] = useState([]);
  const [selectedFleurietCvm, setSelectedFleurietCvm] = useState('');
  const [fleurietStartYear, setFleurietStartYear] = useState('2020');
  const [fleurietEndYear, setFleurietEndYear] = useState('2024');
  const [fleurietResults, setFleurietResults] = useState(null);
  const [fleurietChartInstance, setFleurietChartInstance] = useState(null);

  const [activeTab, setActiveTab] = useState('fleuriet-input');

  // Instâncias dos gráficos Chart.js (Valuation)
  const [valuationScatterChartInstance, setValuationScatterChartInstance] = useState(null);
  const [valuationTop10ChartInstance, setValuationTop10ChartInstance] = useState(null);
  
  // Formatação de valores
  const formatCurrency = (value, prefix = 'R$') => {
    if (value === null || isNaN(value)) return 'N/A';
    if (Math.abs(value) >= 1e9) return `${prefix} ${(value / 1e9).toFixed(2)}B`;
    if (Math.abs(value) >= 1e6) return `${prefix} ${(value / 1e6).toFixed(2)}M`;
    if (Math.abs(value) >= 1e3) return `${prefix} ${(value / 1e3).toFixed(2)}K`;
    return `${prefix} ${value.toFixed(2)}`;
  };

  const formatPercentage = (value) => {
    if (value === null || isNaN(value)) return 'N/A';
    return `${value.toFixed(2)}%`;
  };

  const formatDate = (isoString) => {
    if (!isoString) return 'N/A';
    const date = new Date(isoString);
    return date.toLocaleDateString('pt-BR');
  };

  // --- Funções de Fetch da API ---
  const fetchApi = useCallback(async (endpoint, options = {}) => {
    setLoading(true);
    setError('');
    try {
      const response = await fetch(`${API_BASE_URL}${endpoint}`, options);
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ message: 'Erro desconhecido na API' }));
        throw new Error(errorData.error || errorData.message || `Erro na requisição: ${response.statusText}`);
      }
      return await response.json();
    } catch (err) {
      console.error("Erro ao buscar dados:", err);
      setError(`Erro: ${err.message}. Por favor, tente novamente.`);
      return null;
    } finally {
      setLoading(false);
    }
  }, []);

  // --- Lógica de Valuation ---
  const handleRunValuationQuickAnalysis = async () => {
    const report = await fetchApi('/financial/analyze/complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ num_companies: 15 }),
    });
    if (report) {
      setValuationReport(report);
      setValuationCompanyData(null);
    }
  };

  const handleRunValuationFullAnalysis = async () => {
    const report = await fetchApi('/financial/analyze/complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ num_companies: null }),
    });
    if (report) {
      setValuationReport(report);
      setValuationCompanyData(null);
    }
  };

  const handleSearchValuationCompany = async () => {
    if (!valuationTickerInput) {
      setError('Por favor, insira um ticker para buscar na análise de Valuation.');
      return;
    }
    const data = await fetchApi(`/financial/analyze/company/${valuationTickerInput.toUpperCase().trim()}`);
    if (data) {
      setValuationCompanyData(data);
      setValuationReport(null);
    }
  };

  // --- Lógica de Fleuriet ---
  const fetchFleurietCompanies = useCallback(async () => {
    const companies = await fetchApi('/fleuriet/companies');
    if (companies) {
      setFleurietCompanies(companies);
      if (companies.length > 0) {
        setSelectedFleurietCvm(companies[0].company_id);
      }
    }
  }, [fetchApi]);

  const handleRunFleurietAnalysis = async () => {
    if (!selectedFleurietCvm) {
      setError('Por favor, selecione uma empresa para a análise Fleuriet.');
      return;
    }
    const results = await fetchApi('/fleuriet/analyze', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        cvm_code: selectedFleurietCvm,
        start_year: parseInt(fleurietStartYear),
        end_year: parseInt(fleurietEndYear),
      }),
    });
    if (results) {
      setFleurietResults(results);
    }
  };

  // --- Efeitos para Carregamento Inicial e Gráficos ---
  useEffect(() => {
    fetchFleurietCompanies();
  }, [fetchFleurietCompanies]);

  useEffect(() => {
    if (valuationReport?.full_report_data) {
        const scatterCtx = document.getElementById('valuationScatterChart');
        if (scatterCtx) {
            if (valuationScatterChartInstance) valuationScatterChartInstance.destroy();
            const chartData = valuationReport.full_report_data.map(c => ({ x: c.eva_percentual, y: c.efv_percentual, label: c.ticker }));
            const newChart = new Chart(scatterCtx, {
                type: 'scatter', data: { datasets: [{ label: 'Empresas', data: chartData, backgroundColor: 'rgba(59, 130, 246, 0.7)' }] },
                options: { responsive: true, maintainAspectRatio: false, scales: { x: { title: { display: true, text: 'EVA (%)' } }, y: { title: { display: true, text: 'EFV (%)' } } } }
            });
            setValuationScatterChartInstance(newChart);
        }
    }
    if (valuationReport?.rankings?.top_10_combined) {
        const top10Ctx = document.getElementById('valuationTop10Chart');
        if (top10Ctx) {
            if (valuationTop10ChartInstance) valuationTop10ChartInstance.destroy();
            const top10Data = [...valuationReport.rankings.top_10_combined].sort((a, b) => a.combined_score - b.combined_score);
            const newChart = new Chart(top10Ctx, {
                type: 'bar', data: { labels: top10Data.map(c => c.ticker), datasets: [{ label: 'Score Combinado', data: top10Data.map(c => c.combined_score), backgroundColor: 'rgba(59, 130, 246, 0.8)' }] },
                options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
            });
            setValuationTop10ChartInstance(newChart);
        }
    }
  }, [valuationReport, activeTab]);

  useEffect(() => {
    if (fleurietResults?.chart_data) {
      const ctx = document.getElementById('fleurietChart');
      if (!ctx) return;
      if (fleurietChartInstance) fleurietChartInstance.destroy();
      const chartData = fleurietResults.chart_data;
      const newFleurietChart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: chartData.labels,
          datasets: [
            { label: 'NCG', data: chartData.ncg, borderColor: '#3b82f6', borderWidth: 2, fill: false, tension: 0.1 },
            { label: 'CDG', data: chartData.cdg, borderColor: '#10b981', borderWidth: 2, fill: false, tension: 0.1 },
            { label: 'Tesouraria', data: chartData.t, borderColor: '#8b5cf6', borderWidth: 2, borderDash: [5, 5], fill: false, tension: 0.1 }
          ]
        },
        options: { responsive: true, maintainAspectRatio: false }
      });
      setFleurietChartInstance(newFleurietChart);
    }
  }, [fleurietResults, activeTab]);

  // --- COMPONENTES DE RENDERIZAÇÃO ---
  const RenderValuationRankingTable = ({ data, title, description }) => {
    const [sortedData, setSortedData] = useState(data);
    const [sortConfig, setSortConfig] = useState({ key: 'combined_score', direction: 'descending' });

    useEffect(() => {
        let sortableItems = [...data];
        sortableItems.sort((a, b) => {
            if (a[sortConfig.key] < b[sortConfig.key]) return sortConfig.direction === 'ascending' ? -1 : 1;
            if (a[sortConfig.key] > b[sortConfig.key]) return sortConfig.direction === 'ascending' ? 1 : -1;
            return 0;
        });
        setSortedData(sortableItems);
    }, [data, sortConfig]);

    const requestSort = (key) => {
        let direction = 'ascending';
        if (sortConfig.key === key && sortConfig.direction === 'ascending') {
            direction = 'descending';
        }
        setSortConfig({ key, direction });
    };

    return (
        <Card>
            <CardHeader>
                <CardTitle>{title}</CardTitle>
                <CardDescription>{description}</CardDescription>
            </CardHeader>
            <CardContent>
                <div className="overflow-x-auto">
                    <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                        <thead className="bg-gray-50 dark:bg-gray-800">
                            <tr>
                                {['ticker', 'company_name', 'combined_score', 'eva_percentual', 'efv_percentual', 'upside_percentual'].map(key => (
                                    <th key={key} onClick={() => requestSort(key)} className="table-header-sortable">
                                        {key.replace(/_/g, ' ').replace('percentual', '%')}
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
                            {sortedData.map((company) => (
                                <tr key={company.ticker} className="hover:bg-gray-50 dark:hover:bg-gray-800">
                                    <td className="td-style font-medium">{company.ticker}</td>
                                    <td className="td-style">{company.company_name}</td>
                                    <td className="td-style font-bold text-blue-500">{company.combined_score?.toFixed(2)}</td>
                                    <td className={`td-style ${company.eva_percentual > 0 ? 'text-green-500' : 'text-red-500'}`}>{formatPercentage(company.eva_percentual)}</td>
                                    <td className={`td-style ${company.efv_percentual > 0 ? 'text-blue-500' : 'text-red-500'}`}>{formatPercentage(company.efv_percentual)}</td>
                                    <td className={`td-style ${company.upside_percentual > 0 ? 'text-green-500' : 'text-red-500'}`}>{formatPercentage(company.upside_percentual)}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </CardContent>
        </Card>
    );
  };

  const RenderFleurietAnalysis = () => {
    if (!fleurietResults) return null;
    const { company_name, cvm_code, start_year, end_year, results, chart_data } = fleurietResults;
    return (
        <Card>
            <CardHeader>
                <CardTitle>Resultados para {company_name}</CardTitle>
                <CardDescription>Análise de {start_year} a {end_year}</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
                <div className="chart-container"><canvas id="fleurietChart"></canvas></div>
                <Alert variant={results.situacao_financeira.includes('Saudável') ? 'default' : 'destructive'}>
                    <AlertTitle>Diagnóstico</AlertTitle>
                    <AlertDescription>{results.interpretacao}</AlertDescription>
                </Alert>
            </CardContent>
        </Card>
    );
  };

  // ==============================================================================
  // --- SEÇÃO DE RENDERIZAÇÃO (JSX) REESTRUTURADA ---
  // ==============================================================================
  return (
    <div className="min-h-screen bg-gray-100 dark:bg-gray-900 text-gray-800 dark:text-gray-200 font-sans">
      <div className="container mx-auto p-4 md:p-6 lg:p-8">
        
        <header className="text-center mb-10">
          <h1 className="text-4xl md:text-5xl font-extrabold text-gray-900 dark:text-white tracking-tight">Análise 360°</h1>
          <p className="text-lg text-gray-600 dark:text-gray-400 mt-2">
            Integração do Modelo Fleuriet com Análise de Valuation
          </p>
        </header>

        {loading && (
          <div className="fixed top-4 right-4 flex items-center bg-blue-600 text-white text-sm font-bold px-4 py-2 rounded-full shadow-lg z-50 animate-pulse">
            <Loader2 className="mr-2 h-5 w-5 animate-spin" />
            Processando...
          </div>
        )}
        {error && (
          <Alert variant="destructive" className="mb-6 max-w-3xl mx-auto">
            <AlertTitle>Ocorreu um Erro</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        <Tabs defaultValue="fleuriet-input" className="w-full" onValueChange={setActiveTab}>
          <TabsList className="grid w-full grid-cols-2 mb-6 bg-gray-200 dark:bg-gray-800 p-1 rounded-lg">
            <TabsTrigger value="fleuriet-input" className="flex items-center justify-center gap-2"><Scale />Modelo Fleuriet</TabsTrigger>
            <TabsTrigger value="valuation-dashboard" className="flex items-center justify-center gap-2"><TrendingUp />Análise de Valuation</TabsTrigger>
          </TabsList>

          {/* ==================== ABA FLEURIET ==================== */}
          <TabsContent value="fleuriet-input" className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle>Configurar Análise Fleuriet</CardTitle>
                <CardDescription>Selecione uma empresa e o período para analisar a saúde financeira.</CardDescription>
              </CardHeader>
              <CardContent className="grid grid-cols-1 md:grid-cols-4 gap-4 items-end">
                <div className="md:col-span-2">
                  <Label htmlFor="fleuriet-company">Empresa</Label>
                  <select id="fleuriet-company" value={selectedFleurietCvm} onChange={e => setSelectedFleurietCvm(e.target.value)} className="input-style w-full">
                    {fleurietCompanies.map(c => <option key={c.company_id} value={c.company_id}>{c.ticker} - {c.company_name}</option>)}
                  </select>
                </div>
                <div>
                  <Label htmlFor="fleuriet-start">Ano Início</Label>
                  <select id="fleuriet-start" value={fleurietStartYear} onChange={e => setFleurietStartYear(e.target.value)} className="input-style w-full">
                    {Array.from({ length: 5 }, (_, i) => 2020 + i).map(y => <option key={y} value={y}>{y}</option>)}
                  </select>
                </div>
                <Button onClick={handleRunFleurietAnalysis} disabled={loading} className="w-full">
                  <LineChart className="mr-2 h-4 w-4" /> Analisar
                </Button>
              </CardContent>
            </Card>
            <RenderFleurietAnalysis />
          </TabsContent>

          {/* ==================== ABA VALUATION ==================== */}
          <TabsContent value="valuation-dashboard" className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle>Dashboard de Valuation</CardTitle>
                <CardDescription>Execute uma análise para ver o ranking e a performance das empresas do Ibovespa.</CardDescription>
              </CardHeader>
              <CardContent className="flex flex-col sm:flex-row gap-4">
                <Button onClick={handleRunValuationQuickAnalysis} disabled={loading} className="flex-1">
                  <Zap className="mr-2 h-4 w-4" /> Análise Rápida (Top 15)
                </Button>
                <Button onClick={handleRunValuationFullAnalysis} disabled={loading} variant="secondary" className="flex-1">
                  <Activity className="mr-2 h-4 w-4" /> Análise Completa
                </Button>
              </CardContent>
            </Card>
            
            {valuationReport && (
              <Tabs defaultValue="ranking" className="w-full">
                <TabsList className="grid w-full grid-cols-3">
                  <TabsTrigger value="ranking"><Table className="mr-2 h-4 w-4"/>Ranking Completo</TabsTrigger>
                  <TabsTrigger value="scatter"><PieChart className="mr-2 h-4 w-4"/>Dispersão EVA/EFV</TabsTrigger>
                  <TabsTrigger value="top10"><BarChart3 className="mr-2 h-4 w-4"/>Top 10 Score</TabsTrigger>
                </TabsList>
                <TabsContent value="ranking" className="mt-4">
                  <RenderValuationRankingTable data={valuationReport.full_report_data} title="Ranking Completo" description="Clique nos cabeçalhos para ordenar." />
                </TabsContent>
                <TabsContent value="scatter" className="mt-4">
                  <Card><CardContent className="pt-6"><div className="chart-container"><canvas id="valuationScatterChart"></canvas></div></CardContent></Card>
                </TabsContent>
                <TabsContent value="top10" className="mt-4">
                  <Card><CardContent className="pt-6"><div className="chart-container"><canvas id="valuationTop10Chart"></canvas></div></CardContent></Card>
                </TabsContent>
              </Tabs>
            )}

            <Card>
                <CardHeader>
                    <CardTitle>Análise Individual por Ticker</CardTitle>
                </CardHeader>
                <CardContent className="flex flex-col sm:flex-row gap-4 items-end">
                    <div className="flex-grow">
                        <Label htmlFor="ticker-search">Ticker (ex: VALE3.SA)</Label>
                        <Input id="ticker-search" type="text" value={valuationTickerInput} onChange={e => setValuationTickerInput(e.target.value)} placeholder="Digite o ticker..." className="input-style" />
                    </div>
                    <Button onClick={handleSearchValuationCompany} disabled={loading}><Search className="mr-2 h-4 w-4"/>Buscar</Button>
                </CardContent>
            </Card>

            {valuationCompanyData && (
                <Card>
                    <CardHeader>
                        <CardTitle>Detalhes de {valuationCompanyData.ticker}</CardTitle>
                        <CardDescription>{valuationCompanyData.company_name}</CardDescription>
                    </CardHeader>
                    <CardContent className="grid grid-cols-2 md:grid-cols-3 gap-4">
                        {Object.entries(valuationCompanyData.metrics).map(([key, value]) => (
                            <div key={key} className="bg-gray-50 dark:bg-gray-800 p-4 rounded-lg">
                                <p className="text-sm font-medium text-gray-500 dark:text-gray-400 capitalize">{key.replace(/_/g, ' ')}</p>
                                <p className="text-xl font-bold text-gray-900 dark:text-white">
                                    {typeof value === 'number' ? (key.includes('percentual') ? formatPercentage(value) : formatCurrency(value)) : String(value)}
                                </p>
                            </div>
                        ))}
                    </CardContent>
                </Card>
            )}

          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}

// FIM DO ARQUIVO - GARANTIA DE NÃO TRUNCAMENTO
export default App;
