import { useState, useEffect, useCallback } from 'react';
import { Button } from './components/ui/button.jsx';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './components/ui/card.jsx';
import { Input } from './components/ui/input.jsx';
import { Label } from './components/ui/label.jsx';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './components/ui/tabs.jsx';
import { Badge } from './components/ui/badge.jsx';
import { Alert, AlertDescription } from './components/ui/alert.jsx';
import { Loader2, TrendingUp, TrendingDown, DollarSign, BarChart3, PieChart, Target, Info, Zap, Activity, Flask, Scale } from 'lucide-react'; // Adicione Scale para Fleuriet
import Chart from 'chart.js/auto'; // Importa Chart.js

import './App.css'; // Estilos CSS adicionais para o App
import './index.css'; // Importa o TailwindCSS gerado

// A URL base da API deve ser relativa para funcionar tanto em desenvolvimento quanto em produ��o no Render
const API_BASE_URL = window.location.origin + '/api'; // Agora a base � /api, pois temos /api/financial e /api/fleuriet

function App() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  
  // --- Estados para Valuation ---
  const [valuationReport, setValuationReport] = useState(null); // Para relat�rio completo/r�pido de Valuation
  const [valuationCompanyData, setValuationCompanyData] = useState(null); // Para an�lise de empresa �nica de Valuation
  const [valuationTickerInput, setValuationTickerInput] = useState(''); // Input para buscar empresa �nica Valuation

  // --- Estados para Fleuriet ---
  const [fleurietCompanies, setFleurietCompanies] = useState([]); // Lista de empresas para o dropdown do Fleuriet
  const [selectedFleurietCvm, setSelectedFleurietCvm] = useState(''); // CVM selecionado no Fleuriet
  const [fleurietStartYear, setFleurietStartYear] = useState('2020'); // Ano inicial Fleuriet
  const [fleurietEndYear, setFleurietEndYear] = useState('2024'); // Ano final Fleuriet
  const [fleurietResults, setFleurietResults] = useState(null); // Resultados da an�lise Fleuriet
  const [fleurietChartInstance, setFleurietChartInstance] = useState(null); // Inst�ncia do gr�fico Fleuriet

  const [activeTab, setActiveTab] = useState('valuation-dashboard'); // Aba ativa padr�o

  // Inst�ncias dos gr�ficos Chart.js (Valuation)
  const [valuationScatterChartInstance, setValuationScatterChartInstance] = useState(null);
  const [valuationTop10ChartInstance, setValuationTop10ChartInstance] = useState(null);
  
  // Formata��o de valores
  const formatCurrency = (value, prefix = 'R$') => {
    if (value === null || isNaN(value)) return 'N/A';
    if (Math.abs(value) >= 1e9) {
      return `${prefix} ${(value / 1e9).toFixed(2)}B`;
    }
    if (Math.abs(value) >= 1e6) {
      return `${prefix} ${(value / 1e6).toFixed(2)}M`;
    }
    if (Math.abs(value) >= 1e3) {
      return `${prefix} ${(value / 1e3).toFixed(2)}K`;
    }
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

  // --- Fun��es de Fetch da API ---
  const fetchApi = useCallback(async (endpoint, options = {}) => {
    setLoading(true);
    setError('');
    try {
      const response = await fetch(`${API_BASE_URL}${endpoint}`, options);
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ message: 'Erro desconhecido na API' }));
        throw new Error(errorData.error || errorData.message || `Erro na requisi��o: ${response.statusText}`);
      }
      const data = await response.json();
      return data;
    } catch (err) {
      console.error("Erro ao buscar dados:", err);
      setError(`Erro: ${err.message}. Por favor, tente novamente.`);
      return null;
    } finally {
      setLoading(false);
    }
  }, []); // Depend�ncias vazias, pois API_BASE_URL � constante

  // --- L�gica de Valuation ---
  const handleRunValuationQuickAnalysis = async () => {
    const report = await fetchApi('/financial/analyze/complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ num_companies: 15 }),
    });
    if (report) {
      setValuationReport(report);
      setValuationCompanyData(null);
      setActiveTab('valuation-dashboard');
    }
  };

  const handleRunValuationFullAnalysis = async () => {
    const report = await fetchApi('/financial/analyze/complete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ num_companies: null }), // Ou apenas {}, para todas
    });
    if (report) {
      setValuationReport(report);
      setValuationCompanyData(null);
      setActiveTab('valuation-dashboard');
    }
  };

  const handleSearchValuationCompany = async () => {
    if (!valuationTickerInput) {
      setError('Por favor, insira um ticker para buscar na an�lise de Valuation.');
      return;
    }
    const data = await fetchApi(`/financial/analyze/company/${valuationTickerInput.toUpperCase().trim()}`);
    if (data) {
      setValuationCompanyData(data);
      setValuationReport(null);
      setActiveTab('valuation-company-details');
    }
  };

  // --- L�gica de Fleuriet ---
  const fetchFleurietCompanies = useCallback(async () => {
    const companies = await fetchApi('/fleuriet/companies');
    if (companies) {
      setFleurietCompanies(companies);
      if (companies.length > 0) {
        setSelectedFleurietCvm(companies[0].cvm_code); // Seleciona a primeira por padr�o (usa cvm_code)
      }
    }
  }, [fetchApi]);

  const handleRunFleurietAnalysis = async () => {
    if (!selectedFleurietCvm || !fleurietStartYear || !fleurietEndYear) {
      setError('Por favor, selecione uma empresa e os anos para a an�lise Fleuriet.');
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
      setActiveTab('fleuriet-results');
    }
  };

  // --- Efeitos para Carregamento Inicial e Gr�ficos ---
  useEffect(() => {
    // Carrega a lista de empresas para Fleuriet ao montar o componente
    fetchFleurietCompanies();
  }, [fetchFleurietCompanies]);

  // Efeito para gr�ficos de Valuation
  useEffect(() => {
    // Dashboard Scatter Chart
    if (activeTab === 'valuation-dashboard' && valuationReport && valuationReport.full_report_data) {
      const ctx = document.getElementById('valuationScatterChart');
      if (!ctx) return;

      if (valuationScatterChartInstance) {
        valuationScatterChartInstance.destroy();
      }

      const chartData = valuationReport.full_report_data.map(c => ({
        x: c.eva_percentual,
        y: c.efv_percentual,
        label: c.ticker,
        company_name: c.company_name,
      }));

      const newScatterChart = new Chart(ctx.getContext('2d'), {
        type: 'scatter',
        data: {
          datasets: [{
            label: 'Empresas',
            data: chartData,
            backgroundColor: 'rgba(59, 130, 246, 0.7)', // blue-500
            borderColor: 'rgba(37, 99, 235, 1)', // blue-600
            pointRadius: 6,
            pointHoverRadius: 9,
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                label: function(context) {
                  const company = context.raw;
                  return `${company.label} (${company.company_name}): EVA: ${company.x?.toFixed(2)}%, EFV: ${company.y?.toFixed(2)}%`;
                }
              }
            }
          },
          scales: {
            x: {
              title: {
                display: true,
                text: 'EVA (%) - Cria��o de Valor Atual',
                font: { weight: 'bold' }
              },
              grid: { color: '#e2e8f0' }
            },
            y: {
              title: {
                display: true,
                text: 'EFV (%) - Potencial de Valor Futuro',
                font: { weight: 'bold' }
              },
              grid: { color: '#e2e8f0' }
            }
          }
        }
      });
      setValuationScatterChartInstance(newScatterChart);
    }

    // Top 10 Bar Chart
    if (activeTab === 'valuation-top10' && valuationReport && valuationReport.rankings && valuationReport.rankings.top_10_combined) {
      const ctx = document.getElementById('valuationTop10Chart');
      if (!ctx) return;

      if (valuationTop10ChartInstance) {
        valuationTop10ChartInstance.destroy();
      }

      const top10Data = valuationReport.rankings.top_10_combined.map(c => ({
        ticker: c.ticker,
        score: c.combined_score
      })).sort((a, b) => a.score - b.score); // Ordena ascendente para o gr�fico de barras horizontal

      const newTop10Chart = new Chart(ctx.getContext('2d'), {
        type: 'bar',
        data: {
          labels: top10Data.map(c => c.ticker),
          datasets: [{
            label: 'Score Combinado',
            data: top10Data.map(c => c.score),
            backgroundColor: 'rgba(59, 130, 246, 0.8)', // blue-500
            borderColor: 'rgba(37, 99, 235, 1)', // blue-600
            borderWidth: 1,
            borderRadius: 5, // Cantos arredondados
          }]
        },
        options: {
          indexAxis: 'y', // Barras horizontais
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                label: function(context) {
                  return `${context.label}: ${context.raw?.toFixed(2)}`;
                }
              }
            }
          },
          scales: {
            x: {
              beginAtZero: true,
              title: {
                display: true,
                text: 'Score de Valuation',
                font: { weight: 'bold' }
              },
              grid: { color: '#e2e8f0' }
            },
            y: {
              grid: { display: false }
            }
          }
        }
      });
      setValuationTop10ChartInstance(newTop10Chart);
    }

    // Limpeza ao desmontar o componente ou mudar a aba
    return () => {
      if (valuationScatterChartInstance) valuationScatterChartInstance.destroy();
      if (valuationTop10ChartInstance) valuationTop10ChartInstance.destroy();
    };
  }, [activeTab, valuationReport, valuationScatterChartInstance, valuationTop10ChartInstance]);

  // Efeito para gr�fico de Fleuriet
  useEffect(() => {
    if (activeTab === 'fleuriet-results' && fleurietResults && fleurietResults.chart_data) {
      const ctx = document.getElementById('fleurietChart');
      if (!ctx) return;

      if (fleurietChartInstance) {
        fleurietChartInstance.destroy();
      }

      const chartData = fleurietResults.chart_data;
      const newFleurietChart = new Chart(ctx.getContext('2d'), {
        type: 'line',
        data: {
          labels: chartData.labels,
          datasets: [
            { label: 'NCG', data: chartData.ncg, borderColor: '#3b82f6', borderWidth: 3, fill: false, tension: 0.1, pointRadius: 5, pointBackgroundColor: '#3b82f6' },
            { label: 'CDG', data: chartData.cdg, borderColor: '#10b981', borderWidth: 3, fill: false, tension: 0.1, pointRadius: 5, pointBackgroundColor: '#10b981' },
            { label: 'Tesouraria', data: chartData.t, borderColor: '#8b5cf6', borderWidth: 2, borderDash: [5, 5], fill: false, tension: 0.1, pointRadius: 5, pointBackgroundColor: '#8b5cf6' }
          ]
        },
        options: { responsive: true, maintainAspectRatio: false }
      });
      setFleurietChartInstance(newFleurietChart);
    }
    return () => {
      if (fleurietChartInstance) fleurietChartInstance.destroy();
    };
  }, [activeTab, fleurietResults, fleurietChartInstance]);


  const renderValuationRankingTable = (data, title, description) => {
    if (!data || data.length === 0) {
      return (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertDescription>Nenhum dado dispon�vel para este ranking.</AlertDescription>
        </Alert>
      );
    }

    // Fun��es de ordena��o local para a tabela completa
    const [sortedData, setSortedData] = useState(data);
    const [sortColumn, setSortColumn] = useState('combined_score');
    const [sortOrder, setSortOrder] = useState('desc'); // 'asc' ou 'desc'

    useEffect(() => {
      // Cria uma c�pia da array antes de ordenar para evitar mutar o estado original diretamente
      const dataToSort = [...data]; 
      dataToSort.sort((a, b) => {
        const valA = a[sortColumn];
        const valB = b[sortColumn];

        // Lida com valores null/NaN para ordena��o
        if (valA === null || isNaN(valA)) return sortOrder === 'asc' ? 1 : -1;
        if (valB === null || isNaN(valB)) return sortOrder === 'asc' ? -1 : 1;
        
        // Compara��o para string ou number
        if (typeof valA === 'string' && typeof valB === 'string') {
          return sortOrder === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
        }
        return sortOrder === 'asc' ? valA - valB : valB - valA;
      });
      setSortedData(dataToSort);
    }, [data, sortColumn, sortOrder]);


    const handleHeaderClick = (column) => {
      if (column === sortColumn) {
        setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
      } else {
        setSortColumn(column);
        setSortOrder('desc'); // Padr�o ao mudar de coluna
      }
    };

    const getSortIndicator = (column) => {
      if (column === sortColumn) {
        return sortOrder === 'asc' ? ' ?' : ' ?';
      }
      return '';
    };

    return (
      <Card>
        <CardHeader>
          <CardTitle>{title}</CardTitle>
          <CardDescription>{description}</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto rounded-lg border border-slate-200 shadow-sm">
            <table className="min-w-full divide-y divide-slate-200">
              <thead className="bg-slate-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Pos.</th>
                  <th className="table-header-sortable px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider" onClick={() => handleHeaderClick('ticker')}>
                    Ticker {getSortIndicator('ticker')}
                  </th>
                  <th className="table-header-sortable px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider" onClick={() => handleHeaderClick('company_name')}>
                    Empresa {getSortIndicator('company_name')}
                  </th>
                  <th className="table-header-sortable px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider" onClick={() => handleHeaderClick('combined_score')}>
                    Score {getSortIndicator('combined_score')}
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    EVA (%)
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    EFV (%)
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    Upside (%)
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    Riqueza Atual
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    Riqueza Futura
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    Market Cap
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-200">
                {sortedData.map((company, index) => (
                  <tr key={company.ticker} className="hover:bg-slate-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-500">{index + 1}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-slate-900">{company.ticker}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-800">{company.company_name}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-semibold text-blue-600">{company.combined_score?.toFixed(2) || 'N/A'}</td>
                    <td className={`px-6 py-4 whitespace-nowrap text-sm ${company.eva_percentual > 0 ? 'text-green-600' : 'text-red-600'}`}>
                      {formatPercentage(company.eva_percentual)}
                    </td>
                    <td className={`px-6 py-4 whitespace-nowrap text-sm ${company.efv_percentual > 0 ? 'text-blue-600' : 'text-red-600'}`}>
                      {formatPercentage(company.efv_percentual)}
                    </td>
                    <td className={`px-6 py-4 whitespace-nowrap text-sm ${company.upside_percentual > 0 ? 'text-green-600' : 'text-red-600'}`}>
                      {formatPercentage(company.upside_percentual)}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-600">{formatCurrency(company.riqueza_atual)}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-600">{formatCurrency(company.riqueza_futura)}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-600">{formatCurrency(company.market_cap)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    );
  };

  const renderFleurietAnalysis = () => {
    if (!fleurietResults) {
      return (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertDescription>Selecione uma empresa e anos e clique em "Analisar" para ver os resultados do Modelo Fleuriet.</AlertDescription>
        </Alert>
      );
    }

    const { company_name, cvm_code, start_year, end_year, results, chart_data } = fleurietResults;

    return (
      <div className="space-y-6">
        <Card>
          <CardHeader>
            <CardTitle>Resultados do Modelo Fleuriet para {company_name} (CVM: {cvm_code})</CardTitle>
            <CardDescription>An�lise de {start_year} a {end_year}</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <h4 className="text-lg font-semibold text-gray-700">Vis�o Geral</h4>
              <p><strong>Situa��o Financeira:</strong> <span className={results.situacao_financeira.includes('Saud�vel') ? 'text-green-600 font-bold' : 'text-red-600 font-bold'}>{results.situacao_financeira}</span></p>
              <p><strong>Interpreta��o:</strong> {results.interpretacao}</p>
              
              <h4 className="text-lg font-semibold text-gray-700 mt-6">Gr�fico Tesoura (NCG vs CDG)</h4>
              <div className="chart-container h-96 w-full">
                <canvas id="fleurietChart"></canvas>
              </div>

              <h4 className="text-lg font-semibold text-gray-700 mt-6">Detalhes por Ano</h4>
              <div className="overflow-x-auto rounded-lg border border-slate-200 shadow-sm">
                <table className="min-w-full divide-y divide-slate-200">
                  <thead className="bg-slate-50">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Ano</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">NCG</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">CDG</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">Tesouraria</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200">
                    {chart_data.labels.map((year, index) => (
                      <tr key={year}>
                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-slate-900">{year}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-600">{formatCurrency(chart_data.ncg[index])}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-600">{formatCurrency(chart_data.cdg[index])}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-600">{formatCurrency(chart_data.t[index])}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  };


  return (
    <div className="min-h-screen bg-slate-50 text-slate-800 antialiased">
      <div className="container mx-auto p-4 md:p-8 max-w-6xl">
        <header className="text-center mb-8">
          <h1 className="text-3xl md:text-4xl font-bold text-slate-900">An�lise 360� de Empresas</h1>
          <p className="text-slate-600 mt-2">Combine a sa�de financeira do Modelo Fleuriet com o potencial de Valuation.</p>
        </header>

        <main>
          {error && (
            <Alert className="mb-4 bg-red-100 border-red-400 text-red-700">
              <Info className="h-4 w-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          {loading && (
            <div className="flex justify-center items-center h-32">
              <Loader2 className="h-8 w-8 animate-spin text-blue-500" />
              <span className="ml-2 text-blue-600">Carregando...</span>
            </div>
          )}

          <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
            <TabsList className="grid w-full grid-cols-3 bg-slate-200"> {/* 3 colunas para as abas principais */}
              <TabsTrigger value="fleuriet-input">Modelo Fleuriet</TabsTrigger>
              <TabsTrigger value="valuation-dashboard">Valuation</TabsTrigger>
              <TabsTrigger value="valuation-company-details" disabled={!valuationCompanyData}>Empresa Detalhada (Valuation)</TabsTrigger>
            </TabsList>

            {/* --- Aba: Modelo Fleuriet (Input) --- */}
            <TabsContent value="fleuriet-input" className="mt-4">
              <p className="text-slate-700 leading-relaxed mb-6">
                Selecione uma empresa e um per�odo para analisar sua sa�de financeira atrav�s do Modelo Fleuriet.
              </p>
              <Card>
                <CardHeader>
                  <CardTitle>Configurar An�lise Fleuriet</CardTitle>
                  <CardDescription>Escolha a empresa e os anos para a an�lise.</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <Label htmlFor="fleuriet_cvm_code">Selecione a Empresa</Label>
                    <select
                      id="fleuriet_cvm_code"
                      value={selectedFleurietCvm}
                      onChange={(e) => setSelectedFleurietCvm(e.target.value)}
                      className="mt-1 block w-full px-3 py-2 border border-slate-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                      disabled={loading || fleurietCompanies.length === 0}
                    >
                      <option value="" disabled>Escolha uma empresa...</option>
                      {fleurietCompanies.map((company) => (
                        <option key={company.cvm_code} value={company.cvm_code}> {/* Usar cvm_code como key e value */}
                          {company.ticker} - {company.company_name}
                        </option>
                      ))}
                    </select>
                    {fleurietCompanies.length === 0 && !loading && (
                      <p className="text-sm text-red-500 mt-1">Nenhuma empresa carregada. Verifique a conex�o com o banco de dados e se h� dados financeiros.</p>
                    )}
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <Label htmlFor="fleuriet_start_year">Ano In�cio</Label>
                      <select
                        id="fleuriet_start_year"
                        value={fleurietStartYear}
                        onChange={(e) => setFleurietStartYear(e.target.value)}
                        className="mt-1 block w-full px-3 py-2 border border-slate-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                        disabled={loading}
                      >
                        {Array.from({ length: 5 }, (_, i) => 2020 + i).map(year => (
                          <option key={year} value={year}>{year}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <Label htmlFor="fleuriet_end_year">Ano Fim</Label>
                      <select
                        id="fleuriet_end_year"
                        value={fleurietEndYear}
                        onChange={(e) => setFleurietEndYear(e.target.value)}
                        className="mt-1 block w-full px-3 py-2 border border-slate-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                        disabled={loading}
                      >
                        {Array.from({ length: 5 }, (_, i) => 2020 + i).map(year => (
                          <option key={year} value={year}>{year}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                  <Button onClick={handleRunFleurietAnalysis} disabled={loading} className="w-full bg-indigo-600 hover:bg-indigo-700 text-white shadow-md">
                    <Scale className="mr-2 h-4 w-4" /> {/* �cone para Fleuriet */}
                    Analisar Fleuriet
                  </Button>
                </CardContent>
              </Card>
            </TabsContent>

            {/* --- Aba: Resultados Fleuriet --- */}
            <TabsContent value="fleuriet-results" className="mt-4">
              <p className="text-slate-700 leading-relaxed mb-6">
                Resultados detalhados da an�lise de sa�de financeira pelo Modelo Fleuriet.
              </p>
              {renderFleurietAnalysis()}
            </TabsContent>

            {/* --- Aba: Valuation Dashboard --- */}
            <TabsContent value="valuation-dashboard" className="mt-4">
              <p className="text-slate-700 leading-relaxed mb-6">
                Bem-vindo ao Painel de An�lise de Valuation. Esta se��o oferece uma vis�o geral das principais m�tricas do Ibovespa, destacando a cria��o de valor e o potencial futuro das empresas.
                Utilize os cart�es de m�tricas para um resumo r�pido e o gr�fico de dispers�o para identificar padr�es e oportunidades no mercado.
              </p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-8"> {/* Layout para bot�es de an�lise Valuation */}
                <Card className="flex-1">
                  <CardHeader>
                    <CardTitle>An�lise R�pida de Valuation</CardTitle>
                    <CardDescription>Analise as 15 principais empresas do Ibovespa.</CardDescription>
                  </CardHeader>
                  <CardContent className="flex flex-col gap-3">
                    <Button onClick={handleRunValuationQuickAnalysis} disabled={loading} className="w-full bg-blue-600 hover:bg-blue-700 text-white shadow-md">
                      <Zap className="mr-2 h-4 w-4" />
                      Executar An�lise R�pida
                    </Button>
                  </CardContent>
                </Card>
                <Card className="flex-1">
                  <CardHeader>
                    <CardTitle>An�lise Completa de Valuation</CardTitle>
                    <CardDescription>Analise todas as empresas do Ibovespa (pode demorar).</CardDescription>
                  </CardHeader>
                  <CardContent className="flex flex-col gap-3">
                    <Button onClick={handleRunValuationFullAnalysis} disabled={loading} className="w-full bg-slate-700 hover:bg-slate-800 text-white shadow-md">
                      <Activity className="mr-2 h-4 w-4" />
                      Executar An�lise Completa
                    </Button>
                  </CardContent>
                </Card>
              </div>

              {valuationReport && valuationReport.summary_statistics ? (
                <div className="space-y-6">
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                    <Card className="metric-card">
                      <h3>Empresas Analisadas</h3>
                      <p>{valuationReport.total_companies_analyzed}</p>
                    </Card>
                    <Card className="metric-card">
                      <h3>Criando Valor (EVA &gt; 0)</h3>
                      <p className="text-green-600">{valuationReport.summary_statistics.positive_eva_count}</p>
                    </Card>
                    <Card className="metric-card">
                      <h3>Potencial Futuro (EFV &gt; 0)</h3>
                      <p className="text-blue-600">{valuationReport.summary_statistics.positive_efv_count}</p>
                    </Card>
                    <Card className="metric-card">
                      <h3>Data da An�lise</h3>
                      <p>{formatDate(valuationReport.timestamp)}</p>
                    </Card>
                  </div>

                  <Card>
                    <CardHeader>
                      <CardTitle>Dispers�o EVA vs. EFV</CardTitle>
                      <CardDescription>
                        Este gr�fico ajuda a identificar empresas com forte cria��o de valor atual (EVA) e alto potencial futuro (EFV).
                        O quadrante superior direito representa as empresas ideais.
                      </CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="chart-container">
                        <canvas id="valuationScatterChart"></canvas>
                      </div>
                    </CardContent>
                  </Card>
                  
                  {valuationReport.portfolio_suggestion && (
                    <Card>
                        <CardHeader>
                            <CardTitle>Sugest�o de Portf�lio (Moderado)</CardTitle>
                            <CardDescription>
                                Uma aloca��o de exemplo baseada em uma combina��o de EVA e EFV.
                            </CardDescription>
                        </CardHeader>
                        <CardContent>
                            <div className="space-y-4">
                                <div className="flex justify-between items-center text-sm font-medium text-slate-700">
                                    <span>EVA do Portf�lio:</span>
                                    <span className={valuationReport.portfolio_suggestion.portfolio_eva_pct > 0 ? 'text-green-600' : 'text-red-600'}>
                                        {formatPercentage(valuationReport.portfolio_suggestion.portfolio_eva_pct)} ({formatCurrency(valuationReport.portfolio_suggestion.portfolio_eva_abs)})
                                    </span>
                                </div>
                                <div className="space-y-2">
                                    {Object.entries(valuationReport.portfolio_suggestion.weights).map(([ticker, weight]) => (
                                        <div key={ticker} className="flex items-center">
                                            <span className="text-sm font-medium text-slate-700 w-1/4">{ticker}</span>
                                            <div className="w-3/4 bg-slate-200 rounded-full h-3">
                                                <div
                                                    className="bg-blue-500 h-3 rounded-full"
                                                    style={{ width: `${(weight * 100).toFixed(1)}%` }}
                                                ></div>
                                            </div>
                                            <span className="ml-2 text-sm font-medium w-12 text-right">
                                                {(weight * 100).toFixed(1)}%
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </CardContent>
                    </Card>
                  )}
                </div>
              ) : (
                <Alert>
                  <Info className="h-4 w-4" />
                  <AlertDescription>Execute uma an�lise (R�pida ou Completa) de Valuation para ver o Dashboard.</AlertDescription>
                </Alert>
              )}
            </TabsContent>

            {/* --- Aba: Valuation Ranking Completo --- */}
            <TabsContent value="valuation-full-ranking" className="mt-4">
                <p className="text-slate-700 leading-relaxed mb-6">
                    Nesta aba, voc� encontra o ranking completo de todas as empresas analisadas, com detalhes sobre cada m�trica de Valuation.
                    Clique nos cabe�alhos das colunas para ordenar a tabela e identificar as empresas que se destacam em diferentes aspectos.
                </p>
              {valuationReport && valuationReport.full_report_data ? (
                renderValuationRankingTable(valuationReport.full_report_data, "Ranking Completo de Valuation", "Ordene pela m�trica desejada para encontrar as melhores oportunidades.")
              ) : (
                <Alert>
                  <Info className="h-4 w-4" />
                  <AlertDescription>Execute uma an�lise (R�pida ou Completa) de Valuation para ver o Ranking Completo.</AlertDescription>
                </Alert>
              )}
            </TabsContent>

            {/* --- Aba: Valuation Top 10 --- */}
            <TabsContent value="valuation-top10" className="mt-4">
                <p className="text-slate-700 leading-relaxed mb-6">
                    Explore aqui as 10 melhores empresas por diferentes crit�rios de Valuation. Esta vis�o consolidada permite uma r�pida identifica��o das empresas com maior potencial de cria��o de valor e valoriza��o futura.
                </p>
              {valuationReport && valuationReport.rankings ? (
                <div className="space-y-6">
                  <Card>
                    <CardHeader>
                      <CardTitle>Top 10 Melhores A��es (Score Combinado)</CardTitle>
                      <CardDescription>As 10 empresas com melhor pontua��o combinada, considerando EVA, EFV e Upside. Representam as oportunidades mais atraentes.</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="chart-container">
                        <canvas id="valuationTop10Chart"></canvas>
                      </div>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader>
                      <CardTitle>Empresas Criadoras de Valor (EVA &gt; 0)</CardTitle>
                      <CardDescription>Empresas que geraram valor econ�mico positivo para seus acionistas.</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <ul className="list-disc list-inside space-y-2">
                        {valuationReport.opportunities?.value_creators?.length > 0 ? (
                          valuationReport.opportunities.value_creators.map(([ticker, eva_pct]) => (
                            <li key={ticker} className="flex justify-between items-center">
                              <span className="font-medium text-slate-800">{ticker}</span>
                              <Badge className="bg-green-100 text-green-800">{formatPercentage(eva_pct)} EVA</Badge>
                            </li>
                          ))
                        ) : (
                          <li>Nenhuma empresa com EVA positivo encontrada.</li>
                        )}
                      </ul>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader>
                      <CardTitle>Empresas com Potencial de Crescimento (EFV &gt; 0)</CardTitle>
                      <CardDescription>Empresas com expectativa de cria��o de valor futuro.</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <ul className="list-disc list-inside space-y-2">
                        {valuationReport.opportunities?.growth_potential?.length > 0 ? (
                          valuationReport.opportunities.growth_potential.map(([ticker, efv_pct]) => (
                            <li key={ticker} className="flex justify-between items-center">
                              <span className="font-medium text-slate-800">{ticker}</span>
                              <Badge className="bg-blue-100 text-blue-800">{formatPercentage(efv_pct)} EFV</Badge>
                            </li>
                          ))
                        ) : (
                          <li>Nenhuma empresa com EFV positivo encontrada.</li>
                        )}
                      </ul>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader>
                      <CardTitle>Empresas Subvalorizadas (Upside &gt; 20%)</CardTitle>
                      <CardDescription>A��es com potencial significativo de valoriza��o de acordo com o modelo.</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <ul className="list-disc list-inside space-y-2">
                        {valuationReport.opportunities?.undervalued?.length > 0 ? (
                          valuationReport.opportunities.undervalued.map(([ticker, upside_pct]) => (
                            <li key={ticker} className="flex justify-between items-center">
                              <span className="font-medium text-slate-800">{ticker}</span>
                              <Badge className="bg-purple-100 text-purple-800">{formatPercentage(upside_pct)} Upside</Badge>
                            </li>
                          ))
                        ) : (
                          <li>Nenhuma empresa subvalorizada encontrada.</li>
                        )}
                      </ul>
                    </CardContent>
                  </Card>
                </div>
              ) : (
                <Alert>
                  <Info className="h-4 w-4" />
                  <AlertDescription>Execute uma an�lise (R�pida ou Completa) de Valuation para ver os rankings TOP 10.</AlertDescription>
                </Alert>
              )}
            </TabsContent>

            {/* --- Aba: Valuation Empresa Detalhada --- */}
            <TabsContent value="valuation-company-details" className="mt-4">
                <p className="text-slate-700 leading-relaxed mb-6">
                    Esta se��o apresenta os detalhes da an�lise de valuation para a empresa espec�fica que voc� buscou. Aqui, voc� pode mergulhar nas m�tricas de EVA, EFV, Riqueza e Upside, entendendo a sa�de financeira e o potencial da companhia individualmente.
                </p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-8"> {/* Input para buscar empresa Valuation */}
                <Card className="flex-1 col-span-full">
                  <CardHeader>
                    <CardTitle>An�lise de Valuation por Ticker</CardTitle>
                    <CardDescription>Obtenha uma an�lise detalhada de Valuation para uma empresa espec�fica.</CardDescription>
                  </CardHeader>
                  <CardContent className="flex flex-col gap-3">
                    <Label htmlFor="valuation_ticker">Ticker da Empresa (Ex: PETR4.SA)</Label>
                    <Input
                      id="valuation_ticker"
                      type="text"
                      placeholder="Ex: VALE3.SA"
                      value={valuationTickerInput}
                      onChange={(e) => setValuationTickerInput(e.target.value)}
                      className="border-slate-300 focus:ring-blue-500 focus:border-blue-500"
                    />
                    <Button onClick={handleSearchValuationCompany} disabled={loading} className="w-full bg-blue-600 hover:bg-blue-700 text-white shadow-md">
                      <Info className="mr-2 h-4 w-4" />
                      Buscar Empresa (Valuation)
                    </Button>
                  </CardContent>
                </Card>
              </div>
              {valuationCompanyData ? (
                <Card>
                  <CardHeader>
                    <CardTitle>{valuationCompanyData.company_name} ({valuationCompanyData.ticker})</CardTitle>
                    <CardDescription>An�lise Detalhada de Valuation</CardDescription>
                  </CardHeader>
                  <CardContent className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                    <div className="metric-card p-4">
                      <h3 className="text-sm font-medium text-slate-500">Market Cap</h3>
                      <p className="text-2xl font-bold text-slate-900">{formatCurrency(valuationCompanyData.metrics.market_cap)}</p>
                    </div>
                    <div className="metric-card p-4">
                      <h3 className="text-sm font-medium text-slate-500">Pre�o da A��o</h3>
                      <p className="text-2xl font-bold text-slate-900">{formatCurrency(valuationCompanyData.metrics.stock_price, 'R$ ')}</p>
                    </div>
                    <div className="metric-card p-4">
                      <h3 className="text-sm font-medium text-slate-500">WACC</h3>
                      <p className={`text-2xl font-bold ${valuationCompanyData.metrics.wacc_percentual !== null ? 'text-slate-900' : 'text-slate-500'}`}>
                        {formatPercentage(valuationCompanyData.metrics.wacc_percentual)}
                      </p>
                    </div>
                    <div className="metric-card p-4">
                      <h3 className="text-sm font-medium text-slate-500">EVA (Absoluto)</h3>
                      <p className={`text-2xl font-bold ${valuationCompanyData.metrics.eva_abs > 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {formatCurrency(valuationCompanyData.metrics.eva_abs)}
                      </p>
                    </div>
                    <div className="metric-card p-4">
                      <h3 className="text-sm font-medium text-slate-500">EVA (%)</h3>
                      <p className={`text-2xl font-bold ${valuationCompanyData.metrics.eva_percentual > 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {formatPercentage(valuationCompanyData.metrics.eva_percentual)}
                      </p>
                    </div>
                    <div className="metric-card p-4">
                      <h3 className="text-sm font-medium text-slate-500">EFV (Absoluto)</h3>
                      <p className={`text-2xl font-bold ${valuationCompanyData.metrics.efv_abs > 0 ? 'text-blue-600' : 'text-red-600'}`}>
                        {formatCurrency(valuationCompanyData.metrics.efv_abs)}
                      </p>
                    </div>
                    <div className="metric-card p-4">
                      <h3 className="text-sm font-medium text-slate-500">EFV (%)</h3>
                      <p className={`text-2xl font-bold ${valuationCompanyData.metrics.efv_percentual > 0 ? 'text-blue-600' : 'text-red-600'}`}>
                        {formatPercentage(valuationCompanyData.metrics.efv_percentual)}
                      </p>
                    </div>
                    <div className="metric-card p-4">
                      <h3 className="text-sm font-medium text-slate-500">Riqueza Atual</h3>
                      <p className={`text-2xl font-bold ${valuationCompanyData.metrics.riqueza_atual > 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {formatCurrency(valuationCompanyData.metrics.riqueza_atual)}
                      </p>
                    </div>
                    <div className="metric-card p-4">
                      <h3 className="text-sm font-medium text-slate-500">Riqueza Futura</h3>
                      <p className={`text-2xl font-bold ${valuationCompanyData.metrics.riqueza_futura > 0 ? 'text-blue-600' : 'text-red-600'}`}>
                        {formatCurrency(valuationCompanyData.metrics.riqueza_futura)}
                      </p>
                    </div>
                    <div className="metric-card p-4 col-span-full">
                      <h3 className="text-sm font-medium text-slate-500">Upside Potencial</h3>
                      <p className={`text-2xl font-bold ${valuationCompanyData.metrics.upside_percentual > 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {formatPercentage(valuationCompanyData.metrics.upside_percentual)}
                      </p>
                    </div>
                  </CardContent>
                </Card>
              ) : (
                <Alert>
                  <Info className="h-4 w-4" />
                  <AlertDescription>Nenhuma empresa selecionada. Use a caixa de busca acima.</AlertDescription>
                </Alert>
              )}
            </TabsContent>
          </Tabs>
        </main>
      </div>
    </div>
  );
}

export default App;
