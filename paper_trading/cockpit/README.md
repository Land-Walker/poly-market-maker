# MM 조종석 (Cockpit) — 실시간 페이퍼 트레이딩 계기판 + 조종간

여러 마켓에서 동시에 페이퍼 트레이딩을 돌리면서, 웹 UI에서 파라미터(γ, κ,
order_size, max_inventory)를 실시간으로 만지고 그 반응을 즉시 관찰하는
**학습용** 조종석. 목적은 최적화가 아니라 MM 미시구조 직관 —
spread/inventory/adverse/큐 두께의 인과를 눈으로 보는 것.

**절대 규칙 (기존과 동일): 실거래 경로 없음.** place/cancel/submit·서명·키
코드가 한 줄도 없다 (`test_cockpit.py::test_no_real_order_api_present`가 가드).
원본 `poly_market_maker/`는 미접촉·미import.

## 구조

```
orchestrator.py    MarketRunner(마켓 1개: PaperTrader + async 태스크 + 제어)
                   Orchestrator(전체: add/remove, 손실한도 감시)
server.py          aiohttp 제어 서버 (REST + WS push + 대시보드 서빙)
static/index.html  대시보드 (3열 계기판 + 슬라이더 조종간 + 미니차트)
markets.json       마켓 목록 (token_id를 채워야 함 — 아래 2단계)
logs/              마켓별 tape(.tape.jsonl) / state / 이벤트 로그(.events.jsonl)
```

전부 단일 asyncio 이벤트 루프: 3개 마켓 WS 수신 + 웹서버가 한 프로세스.
파라미터 변경은 공유 `StrategyConfig`를 in-place로 갱신 → 엔진이 매 requote마다
config를 읽으므로 **다음 quote부터 즉시 적용** (+ 큐 리셋 & 즉시 재호가).

## 실행법

### 1. 의존성 (VM에서)

```bash
pip install aiohttp websockets    # 나머지는 표준 라이브러리 + 기존 엔진
```

### 2. 마켓 3개 고르기 (얇은/중간/두꺼운 대조)

```bash
python paper_trading/select_markets.py --liquidity-spread
```

`[THIN]/[MID]/[THICK]` 픽의 YES token_id를 `markets.json`의 `FILL_ME_...`
자리에 붙여넣기. (liquidity = best 큐 두께의 프록시. Hormuz 교훈: 두꺼운
큐는 fill이 안 난다 — thick은 대조군.)

### 3. 서버 기동 (VM에서, 리포 루트 기준)

```bash
python paper_trading/cockpit/server.py \
    --config paper_trading/cockpit/markets.json \
    --port 8080 --push-interval 1.0 --history-len 300
```

### 4. 개인 컴퓨터 브라우저로 접속

**권장: SSH 터널** (포트를 인터넷에 안 열어도 됨):

```bash
# 개인컴에서
ssh -L 8080:localhost:8080 <user>@<VM_IP>
# 브라우저에서 http://localhost:8080/
```

또는 직접 노출(비권장): Oracle Cloud 콘솔 → VCN Security List에 8080 ingress
추가 + VM에서 `sudo iptables -I INPUT -p tcp --dport 8080 -j ACCEPT` 후
`http://<VM_IP>:8080/`. 대시보드에 인증이 없으므로 가급적 터널 사용.

### 5. 조종

* 슬라이더/숫자 입력 → 300ms 디바운스 후 서버 전송 → **즉시 재호가**
  ("applied ... → re-quoted immediately" 확인 메시지)
* 잘못된 값(γ≤0, κ≤0 등)은 서버가 거부하고 빨간 에러로 표시 — 크래시 없음.
  극단값 실험은 자유롭게.
* 마켓별 stop/start 버튼. equity가 `loss_limit` 아래로 떨어지면 자동 정지 +
  배너.
* 계기판: mid/best bid·ask + **best 큐 사이즈(q=)**, 내 quote(사이드 중단 시
  "PULLED"), inventory/cash/equity, PnL 3분해, fill 목록, 미니차트 3개
  (inventory 시계열 / spread_capture vs adverse_cost 누적 / mid+내 quote).

## VM 리소스 확인 (무료티어)

```bash
# 실시간: CPU/메모리
top -p $(pgrep -f cockpit/server.py)
# 또는 스냅샷
ps -o pid,%cpu,%mem,rss,etime -p $(pgrep -f cockpit/server.py)
free -h
```

무거우면 (기준: %CPU가 지속 50%+, RSS가 수백 MB):

1. `--push-interval 2.0` 또는 `3.0` (push 빈도 ↓ — 반응 관찰엔 충분)
2. `--history-len 100` (링버퍼 ↓ — 차트가 짧아질 뿐)
3. `markets.json`에서 마켓 수 줄이기 (3 → 2)

tape는 계속 쌓이므로 디스크도 가끔: `du -sh paper_trading/cockpit/logs/`

## 복기 (학습 자산)

* `logs/<slug>.tape.jsonl` — 백테스트 호환 tape. 그대로 `run_backtest` 입력 가능.
* `logs/<slug>.events.jsonl` — start/stop/param_change 기록.
  "내가 뭘 만졌을 때(param_change: old→new, requoted 여부) 뭐가 일어났나"를
  tape의 fill들과 timestamp로 대조하며 복기.

알려진 미세 트레이드오프: 파라미터 변경 시 즉시 재호가가 같은 mid를 한 번 더
관측해 sigma 버퍼에 0-return 1개가 들어감(순간 sigma 미세 하락). 엔진 무수정을
위해 수용 — param_change 이벤트가 로그에 있으니 복기 시 식별 가능.

## 테스트

```bash
# 기존 엔진 38개 (무수정 보존)
python test_data_loader.py && python test_fill_simulator.py && \
python test_strategy.py && python test_analytics.py && \
python paper_trading/test_paper_trader.py
# 조종석 14개 (네트워크·aiohttp 불필요)
python paper_trading/cockpit/test_cockpit.py
# select_markets (liquidity 버킷 포함)
python paper_trading/select_markets.py --self-test
```
