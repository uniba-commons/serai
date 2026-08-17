import './style.css';

type ArtifactState = 'local' | 'partial' | 'remote' | 'pending';

interface Artifact {
  state: ArtifactState;
  id?: number;
  filename?: string;
  tag?: string | null;
  size?: number;
  who?: string;
  progress?: number | null;
  key?: string;
}

interface CourtyardResponse {
  serai: string;
  artifacts: Artifact[];
}

const app = document.querySelector<HTMLDivElement>('#app')!;
app.innerHTML = `
  <h1 id="serai-name">🐪 serai</h1>
  <ul id="artifacts"></ul>
`;

const nameEl = document.getElementById('serai-name')!;
const listEl = document.getElementById('artifacts')!;

function esc(s: string): string {
  const div = document.createElement('div');
  div.textContent = s;
  return div.innerHTML;
}

function render(data: CourtyardResponse): void {
  nameEl.textContent = `🐪 ${data.serai}`;
  document.title = `${data.serai} — serai`;
  listEl.innerHTML = data.artifacts
    .map((a) => {
      if (a.state === 'pending') {
        const idNote = a.id != null ? ` (id ${a.id})` : '';
        return `<li class="pending"><i>the caravan is on its way…${idNote}</i></li>`;
      }
      const file = encodeURIComponent(a.filename ?? '');
      const status =
        a.state === 'partial'
          ? ` · arriving${a.progress != null ? ` ${a.progress}%` : ''}`
          : a.state === 'remote'
            ? ' · on its way'
            : '';
      const name = esc(a.filename ?? '?');
      const label =
        a.state === 'local'
          ? `<a href="/artifacts/${a.id}/${file}" target="_blank"><b>${name}</b></a>`
          : `<b>${name}</b>`;
      return `<li${a.state === 'local' ? '' : ' class="pending"'}>
        ${label}
        <small>${esc(a.who ?? '?')} · ${a.size}b${status}</small>
        ${a.tag ? `<p class="tag">${esc(a.tag)}</p>` : ''}
      </li>`;
    })
    .join('');
}

async function refresh(): Promise<void> {
  try {
    const res = await fetch('/api/artifacts');
    if (!res.ok) return;
    render((await res.json()) as CourtyardResponse);
  } catch {
    // agent not reachable — keep the last view
  }
}

refresh();
setInterval(refresh, 2000);
