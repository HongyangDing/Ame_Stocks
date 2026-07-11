"use client";

import { useEffect, useState } from "react";

type HealthState = "checking" | "online" | "offline";

interface HealthResponse {
  service: string;
  status: "ok";
  version: string;
}

const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:8000";

export function ApiStatus() {
  const [state, setState] = useState<HealthState>("checking");
  const [version, setVersion] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();

    async function checkHealth() {
      try {
        const response = await fetch(`${apiBaseUrl}/healthz`, {
          cache: "no-store",
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error(`health check failed with ${response.status}`);
        }
        const payload = (await response.json()) as HealthResponse;
        if (payload.status !== "ok") {
          throw new Error("health check returned a non-ok status");
        }
        setVersion(payload.version);
        setState("online");
      } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError") {
          return;
        }
        setState("offline");
      }
    }

    void checkHealth();
    return () => controller.abort();
  }, []);

  const label =
    state === "online"
      ? `API ONLINE · v${version}`
      : state === "offline"
        ? "API OFFLINE"
        : "CHECKING API";

  return (
    <span className={`api-status api-status-${state}`} aria-live="polite">
      <span className="status-dot" aria-hidden="true" />
      {label}
    </span>
  );
}
