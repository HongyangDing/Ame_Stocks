import { ApiStatus } from "@/components/api-status";

const modules = [
  {
    eyebrow: "DATA",
    title: "Data Health",
    description: "Coverage, lineage, storage, and QA across Bronze, Silver, and Gold layers.",
  },
  {
    eyebrow: "RESEARCH",
    title: "Factor Library",
    description: "Versioned factor definitions and comparable validation metrics.",
  },
  {
    eyebrow: "ANALYSIS",
    title: "Factor Detail",
    description: "IC, decay, portfolios, exposures, drawdowns, and cost sensitivity.",
  },
  {
    eyebrow: "OPERATIONS",
    title: "Run History",
    description: "Reviewable jobs, artifacts, retries, and immutable publication snapshots.",
  },
];

export default function Home() {
  return (
    <main>
      <nav className="nav-shell" aria-label="Primary navigation">
        <a className="wordmark" href="#top" aria-label="Ame Stocks home">
          <span className="wordmark-mark">A</span>
          <span>AME STOCKS</span>
        </a>
        <ApiStatus />
      </nav>

      <section className="hero" id="top">
        <div className="hero-copy">
          <p className="kicker">RESEARCH INFRASTRUCTURE · STEP 1</p>
          <h1>From raw market data to defensible factor evidence.</h1>
          <p className="lede">
            A reproducible research platform for U.S. equities, designed to make every data
            transformation, factor version, and backtest assumption inspectable.
          </p>
          <div className="contract-row" aria-label="Contract versions">
            <span>Provider contract v1.1</span>
            <span>Factor contract v1.0</span>
            <span>Mock data only</span>
          </div>
        </div>

        <aside className="milestone-card" aria-label="Current milestone">
          <p className="card-label">CURRENT MILESTONE</p>
          <strong>Foundation ready</strong>
          <p>Application boundaries and public interfaces are now explicit.</p>
          <dl>
            <div>
              <dt>Market API calls</dt>
              <dd>0</dd>
            </div>
            <div>
              <dt>External datasets</dt>
              <dd>0</dd>
            </div>
            <div>
              <dt>Signal schema</dt>
              <dd>3 cols</dd>
            </div>
          </dl>
        </aside>
      </section>

      <section className="module-section" aria-labelledby="platform-modules">
        <div className="section-heading">
          <p className="kicker">PLATFORM MAP</p>
          <h2 id="platform-modules">One evidence chain, four focused views.</h2>
          <p>These routes arrive in Step 5; their source contracts are established first.</p>
        </div>
        <div className="module-grid">
          {modules.map((module, index) => (
            <article className="module-card" key={module.title}>
              <div className="module-index">0{index + 1}</div>
              <p className="card-label">{module.eyebrow}</p>
              <h3>{module.title}</h3>
              <p>{module.description}</p>
              <span className="planned-label">PLANNED · STEP 5</span>
            </article>
          ))}
        </div>
      </section>

      <footer>
        <span>Ame Stocks Research Platform</span>
        <span>No real market data connected</span>
      </footer>
    </main>
  );
}
