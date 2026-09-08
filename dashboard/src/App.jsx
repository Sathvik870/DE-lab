import { useState, useEffect } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  ZAxis,
  PieChart,
  Pie,
  Cell,
  Legend,
} from "recharts";

const API_URL = "http://127.0.0.1:8001";

function MetricCard({
  title,
  value,
  prefix = "",
  suffix = "",
  large = false,
}) {
  return (
    <div
      className={`rounded-2xl border border-slate-800 bg-slate-900 p-5 shadow-lg ${
        large ? "min-h-[150px]" : "min-h-[125px]"
      }`}
    >
      <div className="text-sm font-medium text-slate-400">
        {title}
      </div>

      <div
        className={`mt-6 font-bold tracking-tight text-white ${
          large ? "text-3xl" : "text-2xl"
        }`}
      >
        {prefix}
        {Number(value ?? 0).toLocaleString(undefined, {
          maximumFractionDigits: 2,
        })}
        {suffix}
      </div>
    </div>
  );
}

function ChartCard({ title, children }) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5 shadow-lg">
      <h3 className="mb-4 text-sm font-semibold text-slate-200">
        {title}
      </h3>

      <div className="h-[300px]">
        {children}
      </div>
    </div>
  );
}

function PipelineStatus({ status }) {
  if (!status) {
    return null;
  }

  const success = status.type === "success";

  return (
    <div
      className={`mt-3 inline-flex items-center rounded-lg border px-3 py-2 text-sm ${
        success
          ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-400"
          : "border-red-500/30 bg-red-500/10 text-red-400"
      }`}
    >
      <span
        className={`mr-2 h-2 w-2 rounded-full ${
          success
            ? "bg-emerald-400"
            : "bg-red-400"
        }`}
      />

      {status.message}
    </div>
  );
}

function App() {
  const [eda, setEda] = useState(null);

  const [businessAnalytics, setBusinessAnalytics] =
    useState(null);

  const [selectedFile, setSelectedFile] =
    useState(null);

  const [uploading, setUploading] =
    useState(false);

  const [pipelineStatus, setPipelineStatus] =
    useState(null);

  const RATING_COLORS = [
    "#22c55e",
    "#84cc16",
    "#eab308",
    "#f97316",
    "#ef4444",
  ];

  const handleFileChange = (event) => {
    const file =
      event.target.files?.[0];

    if (!file) {
      return;
    }

    if (
      !file.name
        .toLowerCase()
        .endsWith(".csv")
    ) {
      setPipelineStatus({
        type: "error",
        message:
          "Please select a CSV file.",
      });

      setSelectedFile(null);

      return;
    }

    setSelectedFile(file);

    setPipelineStatus({
      type: "success",
      message: `${file.name} selected`,
    });
  };

  
  const fetchBusinessAnalytics = async () => {
    try {
      const response = await fetch(
        `${API_URL}/business-analytics`
      );

      if (!response.ok) {
        throw new Error(
          "Failed to fetch business analytics"
        );
      }

      const data =
        await response.json();

      console.log(
        "[BUSINESS] Analytics:",
        data
      );

      setBusinessAnalytics(data);
    } catch (error) {
      console.error(
        "[BUSINESS] Error:",
        error
      );
    }
  };

  const handleUpload = async () => {
    if (!selectedFile) {
      setPipelineStatus({
        type: "error",
        message:
          "Please select a CSV file.",
      });

      return;
    }

    const formData =
      new FormData();

    formData.append(
      "file",
      selectedFile
    );

    setUploading(true);

    setPipelineStatus({
      type: "success",
      message:
        "Uploading CSV and running ETL pipeline...",
    });

    try {
      const response =
        await fetch(
          `${API_URL}/upload`,
          {
            method: "POST",
            body: formData,
          }
        );

      const data =
        await response.json();

      if (!response.ok) {
        throw new Error(
          data.detail?.message ||
            data.detail ||
            "Pipeline failed"
        );
      }

      if (
        data.pipeline_status ===
        "completed"
      ) {
        if (data.eda) {
          setEda(data.eda);
        }

        await fetchBusinessAnalytics();

        setPipelineStatus({
          type: "success",
          message:
            "Pipeline completed successfully. Analytics updated.",
        });

        setSelectedFile(null);

        const input =
          document.getElementById(
            "csv-upload"
          );

        if (input) {
          input.value = "";
        }
      } else {
        setPipelineStatus({
          type: "error",
          message:
            "Pipeline failed.",
        });
      }
    } catch (error) {
      console.error(
        "[UPLOAD] Error:",
        error
      );

      setPipelineStatus({
        type: "error",
        message:
          error.message ||
          "Pipeline failed",
      });
    } finally {
      setUploading(false);
    }
  };

  const products =
    businessAnalytics?.products || [];

  useEffect(() => {
    fetchBusinessAnalytics();
  }, []);

  const priceDistribution = [
    {
      range: "0-500",
      count: products.filter(
        (p) => p.price <= 500
      ).length,
    },
    {
      range: "500-1000",
      count: products.filter(
        (p) =>
          p.price > 500 &&
          p.price <= 1000
      ).length,
    },
    {
      range: "1000-1500",
      count: products.filter(
        (p) =>
          p.price > 1000 &&
          p.price <= 1500
      ).length,
    },
    {
      range: "1500-2000",
      count: products.filter(
        (p) =>
          p.price > 1500 &&
          p.price <= 2000
      ).length,
    },
    {
      range: "2000-3000",
      count: products.filter(
        (p) =>
          p.price > 2000 &&
          p.price <= 3000
      ).length,
    },
    {
      range: "3000+",
      count: products.filter(
        (p) => p.price > 3000
      ).length,
    },
  ];

  const discountDistribution = [
    {
      range: "0-10%",
      count: products.filter(
        (p) =>
          p.discount >= 0 &&
          p.discount < 10
      ).length,
    },
    {
      range: "10-20%",
      count: products.filter(
        (p) =>
          p.discount >= 10 &&
          p.discount < 20
      ).length,
    },
    {
      range: "20-30%",
      count: products.filter(
        (p) =>
          p.discount >= 20 &&
          p.discount < 30
      ).length,
    },
    {
      range: "30-40%",
      count: products.filter(
        (p) =>
          p.discount >= 30 &&
          p.discount < 40
      ).length,
    },
    {
      range: "40-50%",
      count: products.filter(
        (p) =>
          p.discount >= 40 &&
          p.discount < 50
      ).length,
    },
    {
      range: "50%+",
      count: products.filter(
        (p) =>
          p.discount >= 50
      ).length,
    },
  ];

  const ratingDistribution = [
    {
      rating: "0-1",
      count: products.filter(
        (p) =>
          p.rating >= 0 &&
          p.rating < 1
      ).length,
    },
    {
      rating: "1-2",
      count: products.filter(
        (p) =>
          p.rating >= 1 &&
          p.rating < 2
      ).length,
    },
    {
      rating: "2-3",
      count: products.filter(
        (p) =>
          p.rating >= 2 &&
          p.rating < 3
      ).length,
    },
    {
      rating: "3-4",
      count: products.filter(
        (p) =>
          p.rating >= 3 &&
          p.rating < 4
      ).length,
    },
    {
      rating: "4-5",
      count: products.filter(
        (p) =>
          p.rating >= 4 &&
          p.rating <= 5
      ).length,
    },
  ];

  const discountSegments = [
    {
      segment: "0-10%",
      min: 0,
      max: 10,
    },
    {
      segment: "10-20%",
      min: 10,
      max: 20,
    },
    {
      segment: "20-30%",
      min: 20,
      max: 30,
    },
    {
      segment: "30-40%",
      min: 30,
      max: 40,
    },
    {
      segment: "40-50%",
      min: 40,
      max: 50,
    },
    {
      segment: "50-60%",
      min: 50,
      max: 60,
    },
    {
      segment: "60%+",
      min: 60,
      max: Infinity,
    },
  ];

  return (
    <div className="min-h-screen bg-slate-950 p-6 text-white">

      <div className="mx-auto max-w-[1800px]">

        <header className="mb-6 flex items-start justify-between border-b border-slate-800 pb-5">

          <div>
            <h1 className="text-2xl font-bold">
              Earbuds Analytics
            </h1>

            <p className="mt-1 text-sm text-slate-400">
              EDA and business analytics
            </p>

            <PipelineStatus
              status={
                pipelineStatus
              }
            />
          </div>

        </header>

        <main className="space-y-8">

          <section className="grid grid-cols-7 gap-4">

            <div className="flex min-h-[150px] flex-col rounded-2xl border-2 border-dashed border-slate-700 bg-slate-900 p-4 transition hover:border-slate-500">

              <div>
                <div className="text-sm font-semibold text-slate-200">
                  Upload CSV
                </div>

                <p className="mt-1 text-xs text-slate-500">
                  Start ETL pipeline
                </p>
              </div>

              <label
                htmlFor="csv-upload"
                className="mt-3 flex flex-1 cursor-pointer items-center justify-center rounded-xl border border-slate-800 bg-slate-950 px-2 transition hover:bg-slate-800"
              >
                <div className="text-center">

                  <div className="text-2xl text-slate-300">
                    ↑
                  </div>

                  <div className="mt-1 max-w-[150px] truncate text-xs text-slate-400">
                    {selectedFile
                      ? selectedFile.name
                      : "Choose CSV"}
                  </div>

                </div>

                <input
                  id="csv-upload"
                  type="file"
                  accept=".csv"
                  className="hidden"
                  onChange={
                    handleFileChange
                  }
                />

              </label>

              <button
                onClick={
                  handleUpload
                }
                disabled={
                  !selectedFile ||
                  uploading
                }
                className="mt-3 w-full rounded-xl bg-white py-2 text-xs font-semibold text-slate-950 transition hover:bg-slate-200 disabled:cursor-not-allowed disabled:opacity-40"
              >
                {uploading
                  ? "Running Pipeline..."
                  : "Upload & Run"}
              </button>

            </div>

            <MetricCard
              title="Total Products"
              value={
                eda?.total_rows
              }
              large
            />

            <MetricCard
              title="Average Price"
              value={
                eda?.average_price
              }
              prefix="₹"
              large
            />

            <MetricCard
              title="Average Discount"
              value={
                eda?.average_discount
              }
              suffix="%"
              large
            />

            <MetricCard
              title="Average Rating"
              value={
                eda?.average_rating
              }
              large
            />

            <MetricCard
              title="Average Rating Count"
              value={
                eda?.average_rating_count
              }
              large
            />

            <MetricCard
              title="Missing Values"
              value={
                eda?.missing_values_percent
              }
              suffix="%"
              large
            />

          </section>

          <div className="border-t border-slate-800" />

          <section>

            <div className="mb-5">

              <h2 className="text-xl font-bold text-white">
                Business Analytics
              </h2>

              <p className="mt-1 text-sm text-slate-400">
                Pricing, popularity, rating and discount strategy analysis
              </p>

            </div>

            <div className="grid grid-cols-7 gap-4">

              <MetricCard
                title="Total Products"
                value={
                  businessAnalytics
                    ?.kpis
                    ?.total_products
                }
              />

              <MetricCard
                title="Average Selling Price"
                value={
                  businessAnalytics
                    ?.kpis
                    ?.average_selling_price
                }
                prefix="₹"
              />

              <MetricCard
                title="Average MRP"
                value={
                  businessAnalytics
                    ?.kpis
                    ?.average_mrp
                }
                prefix="₹"
              />

              <MetricCard
                title="Average Discount %"
                value={
                  businessAnalytics
                    ?.kpis
                    ?.average_discount
                }
                suffix="%"
              />

              <MetricCard
                title="Average Rating"
                value={
                  businessAnalytics
                    ?.kpis
                    ?.average_rating
                }
              />

              <MetricCard
                title="Total Rating Count"
                value={
                  businessAnalytics
                    ?.kpis
                    ?.total_rating_count
                }
              />

              <MetricCard
                title="Avg Rating Count / Product"
                value={
                  businessAnalytics
                    ?.kpis
                    ?.average_rating_count
                }
              />

            </div>

          </section>

          <section>

            <h2 className="mb-4 text-lg font-semibold text-slate-200">
              Price & Competitive Positioning
            </h2>

            <div className="grid grid-cols-3 gap-4">

              <ChartCard title="Price Distribution">

                <ResponsiveContainer
                  width="100%"
                  height="100%"
                >
                  <BarChart
                    data={
                      priceDistribution
                    }
                  >
                    <CartesianGrid
                      strokeDasharray="3 3"
                      stroke="#334155"
                    />

                    <XAxis
                      dataKey="range"
                      stroke="#94a3b8"
                    />

                    <YAxis
                      stroke="#94a3b8"
                    />

                    <Tooltip />

                    <Bar
                      dataKey="count"
                      fill="#60a5fa"
                      radius={[
                        6,
                        6,
                        0,
                        0,
                      ]}
                    />
                  </BarChart>
                </ResponsiveContainer>

              </ChartCard>

              <ChartCard title="Price vs Rating">

                <ResponsiveContainer
                  width="100%"
                  height="100%"
                >
                  <ScatterChart>

                    <CartesianGrid
                      strokeDasharray="3 3"
                      stroke="#334155"
                    />

                    <XAxis
                      type="number"
                      dataKey="price"
                      name="Price"
                      stroke="#94a3b8"
                    />

                    <YAxis
                      type="number"
                      dataKey="rating"
                      name="Rating"
                      domain={[
                        0,
                        5,
                      ]}
                      stroke="#94a3b8"
                    />

                    <ZAxis
                      type="number"
                      dataKey="rating_count"
                      range={[
                        30,
                        300,
                      ]}
                    />

                    <Tooltip />

                    <Scatter
                      data={
                        products
                      }
                      fill="#a78bfa"
                    />

                  </ScatterChart>
                </ResponsiveContainer>

              </ChartCard>

              <ChartCard title="Price vs Rating Count">

                <ResponsiveContainer
                  width="100%"
                  height="100%"
                >
                  <ScatterChart>

                    <CartesianGrid
                      strokeDasharray="3 3"
                      stroke="#334155"
                    />

                    <XAxis
                      type="number"
                      dataKey="price"
                      name="Price"
                      stroke="#94a3b8"
                    />

                    <YAxis
                      type="number"
                      dataKey="rating_count"
                      name="Rating Count"
                      stroke="#94a3b8"
                    />

                    <Tooltip />

                    <Scatter
                      data={
                        products
                      }
                      fill="#34d399"
                    />

                  </ScatterChart>
                </ResponsiveContainer>

              </ChartCard>

            </div>

          </section>

          <section>

          <h2 className="mb-4 text-lg font-semibold text-slate-200">
            Rating & Discount Strategy
          </h2>

          <div className="grid grid-cols-3 gap-4">

            <ChartCard title="Rating Distribution">

              <ResponsiveContainer
                width="100%"
                height="100%"
              >
                <PieChart>

                  <Pie
                    data={ratingDistribution}
                    dataKey="count"
                    nameKey="rating"
                    cx="50%"
                    cy="50%"
                    outerRadius={100}
                    label
                  >
                    {ratingDistribution.map((entry, index) => (
                      <Cell
                        key={`cell-${index}`}
                        fill={RATING_COLORS[index % RATING_COLORS.length]}
                      />
                    ))}
                  </Pie>

                  <Tooltip />

                  <Legend />

                </PieChart>

              </ResponsiveContainer>

            </ChartCard>


            <ChartCard title="Discount Distribution">

              <ResponsiveContainer
                width="100%"
                height="100%"
              >
                <BarChart
                  data={discountDistribution}
                >

                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="#334155"
                  />

                  <XAxis
                    dataKey="range"
                    stroke="#94a3b8"
                  />

                  <YAxis
                    stroke="#94a3b8"
                  />

                  <Tooltip />

                  <Bar
                    dataKey="count"
                    fill="#38bdf8"
                    radius={[
                      6,
                      6,
                      0,
                      0,
                    ]}
                  />

                </BarChart>

              </ResponsiveContainer>

            </ChartCard>


            <ChartCard title="Discount % vs Rating Count">

              <ResponsiveContainer
                width="100%"
                height="100%"
              >
                <ScatterChart>

                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="#334155"
                  />

                  <XAxis
                    type="number"
                    dataKey="discount"
                    name="Discount %"
                    stroke="#94a3b8"
                  />

                  <YAxis
                    type="number"
                    dataKey="rating_count"
                    name="Rating Count"
                    stroke="#94a3b8"
                  />

                  <Tooltip />

                  <Scatter
                    data={products}
                    fill="#f97316"
                  />

                </ScatterChart>

              </ResponsiveContainer>

            </ChartCard>

          </div>

        </section>

        </main>
      </div>
    </div>
  );
}

export default App;