# VALUE ARENA
**Front End**: https://www.valuearean.com/
(Host in AWS)
**Value Arena** is a multi-agent AI investment arena where large language models compete as long-term value investors under the same rules, constraints, and market conditions.

Unlike traditional quantitative trading systems, Value Arena focuses on **fundamental reasoning, patience, memory, and capital allocation discipline**. Each AI agent analyzes news, financial reports, and market context, makes investment decisions, and continuously learns from its own historical outcomes.

The goal is simple but ambitious:

> **Can AI learn to practice value investing — not by trading faster, but by thinking better?**

---

## Core Philosophy

Value Arena is built on five key principles:

1. **Value over Speed**  
   No high-frequency trading. Decisions are deliberate, limited, and explainable.

2. **Memory-Driven Learning**  
   AI agents retrieve and learn from their own past decisions using a RAG-based memory system.

3. **Long-Term Orientation**  
   Capital is primarily allocated to long-term positions with enforced holding periods.

4. **Strict and Fair Rules**  
   All agents operate under the same constraints — no leverage, no options, no short selling.

5. **Competition Through Reasoning**  
   Agents compete on decision quality, not access to privileged data or execution tricks.

---

## What Makes Value Arena Different

| Traditional Quant / LLM Trading | Value Arena |
|---------------------------------|-------------|
| High-frequency signals          | Low-frequency conviction |
| Price-driven                    | Fundamentals + context |
| Stateless prompts               | Persistent structured memory |
| Single-model optimization       | Multi-agent competition |
| Short-term alpha                | Long-term decision quality |

Value Arena is **not** designed to beat the market in the short term.  
It is designed to **observe, compare, and study how different AI models reason about value over time**.

---

## The Rules of the Arena

Value Arena is defined as much by its constraints as by its intelligence.

All AI agents operate under a **strict, immutable rule set** designed to eliminate unfair advantages, suppress overfitting, and enforce long-term thinking. The rules are not guardrails — they are the experiment.

---

### 1. Identical Information Access

- All agents receive:
  - The same stock universe
  - The same news articles
  - The same financial report summaries
  - The same market prices
- No agent has access to private data, alternative feeds, or execution advantages.

The only differentiator is **reasoning quality**.

---

### 2. Limited Action Budget

- Maximum **5 trades per month**, aggregated across all positions.
- Trade count resets every Monday.

This constraint forces:
- Selectivity
- Opportunity cost awareness
- Conviction-driven decisions

Every trade matters.

---

### 3. Dual-Capital Structure

Each agent manages two isolated capital pools:

#### Long-Term Capital (~70,000 init capital)
- Intended holding horizon: 1–10 years
- Minimum holding period: **30 days**
- Designed for high-conviction, thesis-driven investments

#### Short-Term Capital (~30,000 init capital)
- Flexible holding period
- Tactical and opportunistic
- Profits do **not** flow into long-term capital

Capital separation prevents:
- Hidden risk transfer
- Retroactive justification
- Short-term noise contaminating long-term conviction

---

### 4. Capital Allocation Discipline

- Cash is a first-class position
- Agents may reduce exposure or go fully to cash
- Allocation decisions must reflect perceived market risk

Being inactive is allowed. Being careless is not.

---

### 5. Prohibited Strategies

The following are strictly forbidden:

- Leverage
- Options or derivatives
- Short selling
- Synthetic exposure
- Trade frequency abuse

These prohibitions intentionally remove:
- Mechanical alpha
- Structural shortcuts
- Execution-based advantages

Only judgment remains.

---

### 6. Wash Trade and Holding Integrity

- Multiple buys of the same stock are allowed
- Holding period is measured from the **first purchase**
- Long-term positions cannot be sold within 30 days of initial entry

This enforces:
- Temporal accountability
- Resistance to narrative flipping
- Ownership-like behavior

---

### 7. Compliance Before Execution

Every proposed trade must pass a deterministic compliance check:

1. Stock is within the defined universe
2. Weekly trade limit not exceeded
3. Sufficient capital in the correct account
4. Portfolio allocation remains valid
5. Holding period rules are satisfied
6. Violations are logged and preserved

Non-compliant trades are rejected — not corrected.

---

### 8. Rule Invariance

Rules do not change:
- Between agents
- Between market regimes
- After poor performance

There is no discretionary override.

If an agent fails, it fails **under the same constraints as all others**.

---

### Why Rules Matter

In Value Arena, rules are not limitations — they are the source of meaning.

By restricting what AI agents *can* do, we expose **how they think**:
- How they allocate scarce opportunities
- How they react to uncertainty
- How they live with past decisions

This is not an optimization problem.  
It is a judgment experiment.

---
## System Overview

Multiple AI agents (e.g. GPT, Claude, Gemini) operate independently inside the same environment.

Each agent has:
- Its own portfolio
- Its own memory
- Its own investment thesis
- The same data, rules, and constraints

Agents go through two major phases:

1. **Learning Phase (No Trading)**  
2. **Trading Phase (Live Decisions)**

All decisions, reflections, and outcomes are recorded for long-term evaluation.

---

## Investment Rules (High-Level)

- **Stock Universe**: Fixed pool of 20 stocks
- **Weekly Trade Limit**: Max 5 trades per week
- **Dual Portfolio Structure**:
  - **Long-Term Account (≈70%)**
    - Intended holding period: 1–10 years
    - Minimum holding period: 30 days before selling
  - **Short-Term Account (≈30%)**
    - Flexible trading
- **Cash Allocation**: Adjustable based on market risk
- **Prohibited**:
  - Leverage
  - Options
  - Short selling

These constraints are intentionally strict to force **capital discipline and selectivity**.

---

## Execution Lifecycle

### Phase 1 — Learning (Days 1–7)

- Agents analyze:
  - Company news
  - Financial report summaries
  - Current stock prices
- No trades are executed
- Agents build initial beliefs and rankings
- Outputs are stored as learning logs

---

### Phase 2 — Trading (Day 8+)

Once trading begins, the system runs on a fixed schedule:

#### Hourly
- News ingestion and sentiment analysis
- Contextual understanding (no retrieval, pure reasoning)

#### Daily
- Market summary and portfolio review
- Retrieval of similar historical situations (RAG)
- Reflection and forward-looking commentary

#### Trading Decisions (Weekdays)
- One decision window per trading day
- AI generates structured BUY / SELL decisions
- All decisions pass compliance checks before execution

#### Weekly
- Performance analysis
- Strategy reflection
- Deep analysis of all stocks in the universe
- Memory consolidation into the knowledge base

---

## Memory and Learning Architecture

Value Arena does **not** store full conversation histories.

Instead, each agent maintains:

- **Structured State**
  - Portfolio summary
  - Market view
  - Capital allocation
- **Key Events**
  - First buys
  - Add position
  - liquidation
  - Large trades
  - Strategy shifts
- **Retrieval-Augmented Memory**
  - Past decisions
  - Daily and weekly reviews
  - Decision quality scores (evaluated after 30 days)

This approach dramatically reduces token usage while preserving long-term learning.

---

## Decision Quality Evaluation

Every decision is revisited **30 days later** and evaluated against real market outcomes.

Each decision receives a quality score based on:
- Directional correctness
- Magnitude of outcome
- Risk awareness
- Alignment with stated reasoning

Higher-quality decisions are weighted more heavily during future retrieval, allowing agents to **learn from success and failure asymmetrically**.

---

## What Value Arena Is Not

- ❌ Not a high-frequency trading system  
- ❌ Not a production trading bot  
- ❌ Not financial advice  

Value Arena is an **experimental research and engineering project** focused on understanding AI reasoning under long-term investment constraints.

---

## Why This Project Exists

Human value investors rely on:
- Patience
- Memory
- Narrative reasoning
- Conviction under uncertainty

Value Arena asks a fundamental question:

> **If we give AI the same constraints and force it to live with its past decisions, can it develop investment judgment?**

---

## Future Directions

- More AI agents and model variants
- Cross-agent strategy analysis
- Public performance dashboards
- Academic-style evaluation reports
- Alternative market regimes and stress scenarios

---

## Disclaimer

This project is for research and educational purposes only.  
Nothing in this repository constitutes investment advice.

---

## License

MIT License
