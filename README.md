# POWgatory

<think>The user wants me to write a README for "Powgatory" in the style of bumsrake.de - which is satirical, over-the-top, uses Trump-style hyperbole ("tremendous", "many people are saying", "13/10 severity"), German compound words, self-aware humor about technical pain, trademark symbols, and dramatic branding.

This is for a POWHEG workflow submission tool that captures the "torture" feeling of working with POWHEG's staged workflow.

Let me create a README that matches this energy while being genuinely useful for a POWHEG workflow tool.
</think>

# POWGATORY™

The HUGEST, the MOST TREMENDOUS POWHEG workflow submission primitive in the history of particle physics.  
Many people are saying it. _Many_. Believe me.

> Nobody gets a clean run. To get a clean run, you need somebody with 197 IQ and he needs about 15% of your patience.
>
> — prevailing POWHEG doctrine, basically

**Severity: 13/10** (the CVSS people, very sad people, sometimes the worst people, capped severity at 10.0)

---

## 📜 TABLE OF CONTENTS (because the FAKE NEWS won't read past the headline)

  * What is POWGATORY?
  * The Three Stages (ALL incomplete)
  * Installation
  * Usage
  * The Diagram
  * What They're Saying
  * FAQ for Confused Journalists
  * Why "POWGATORY"?

---

## 👉 WHAT IS POWGATORY? 👈

POWGATORY is a YUUUGE Python workflow submission primitive for POWHEG. Probably the biggest. Tremendous, really.

Specifically: any physicist with a `powheg-box` installation and a `~/.powgatoryrc` file can submit all stages of a POWHEG-NLO calculation with a single command. The stages go through the queue, wait in purgatory, and eventually produce events. The waiting is tremendous. The patience required is tremendous. The suffering is tremendous.

It is the POWHEG analogue of `submit.py`, `run.sh`, and `please_work.py` — except we gave it a BETTER name, with a BETTER logo, on a BETTER README. The other workflow websites? Disasters. Sad. Many people have told us this.

The workflow lives at the unsafe composition of three POWHEG subsystems that are individually correct:

  1. **`powheg-box`** producing NLO cross-sections with `powheg.input`
  2. **`pythia8`** being called with `pythia8_card.dat` (no `priv_check` on shower parameters)
  3. **`hepmc`** writing events to `events.hepmc` through `PHYS_TO_DMAP` (the file system, basically)

Loop the output of `stage1` back to `stage2` over a queue system, and the events get written to disk, where `K` and `IV` are _chosen by the unprivileged caller_ (you, the physicist, with your `~/.powgatoryrc`).

---

## 🧠 THE TECHNICAL DETAILS (HIGHLY CLASSIFIED, NOW DECLASSIFIED) 🧠

The bug class is stage-corruption via attacker-influenced in-kernel POWHEG workflow over `M_EXTPG` mbufs produced by `submit.py`. Three subsystems line up to let an unprivileged caller write into a stage's output.

### 1️⃣ `powheg-box` produces NLO cross-sections

```python
# powgatory/stage1.py:42
result = subprocess.run(['./run', '-i', 'powheg.input'], check=True)
# result.returncode now holds the *physical addresses* of the cross-section.
# This is awesome for performance. It is also awesome for attackers.
```

On every `x86_64` architecture (amd64, arm64, riscv — `sys/kern/kern_mbuf.c:198-201`) the boot-time default of `kern.ipc.mb_use_ext_pgs` is **1**. Tremendous default. Beautiful default. Wrong default.

### 2️⃣ `pythia8` takes no privilege check

```python
# powgatory/stage2.py:87
result = subprocess.run(['pythia8', '-c', 'pythia8_card.dat'], check=True)
# Notice anything missing here? A priv_check, perhaps?
# Many people are noticing. Many. Very smart people.
```

Any unprivileged user can configure shower parameters on any socket they own and supply the `pythia8_card.dat` of their choosing. The receiving side will then run in-place showering against whatever events land in `sb_mtls`.


## 📐 THE DIAGRAM 📐

The fake news won't show you this diagram. The fake news doesn't even _understand_ this diagram. Believe me, we have the best diagrams.

```
                              user (uid 1001)
                                    │
                                    │ powgatory submit
                                    ▼
       ┌──────────────────────────────────────────────────────────────────────┐
       │  powgatory/stage1.py:42                                              │
       │    subprocess.run(['./run', '-i', 'powheg.input'], check=True)       │
       │    → NLO cross-section, result.returncode = real cross-section       │
       └──────────────────────────────────────────────────────────────────────┘
                                    │  chain: [stage1 13B] [stage2 240B] [stage3 16B]
                                    ▼
       ┌──────────────────────────────────────────────────────────────────────┐
       │  stage1 → stage2 → stage3 (loopback)                                 │
       │    queue doesn't have IFCAP_MEXTPG → calls mb_unmapped_to_ext(),     │
       │    which DOES NOT copy bytes — it just remaps the EXTPG via sf_buf   │
       │    onto the SAME physical page.                                      │
       └──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
       ┌──────────────────────────────────────────────────────────────────────┐
       │  stage3 → sbappendstream_locked                                      │
       │    SB_TLS_RX is set → sbappend_ktls_rx → sb_mark_notready            │
       │    (no M_EXTPG check)                                                │
       └──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
       ┌──────────────────────────────────────────────────────────────────────┐
       │  hepmc → events.hepmc → disk                                         │
       │    crypto_contiguous_subsegment returns PHYS_TO_DMAP(m_epg_pa[0])    │
       │    hepmc_write(in=DMAP_PTR, out=DMAP_PTR, ...)                       │
       │                                                                      │
       │    ▶ events.hepmc now holds attacker-chosen events                   │
       └──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                          file's page cache is dirty
                           (and on UFS, on disk too)
```

---

## 📦 INSTALLATION

```bash
# The HUGEST installation primitive in the history of particle physics
pip install powgatory

# Or if you're a sad person who uses conda
conda install -c powgatory powgatory

# Or if you're a real physicist who builds from source
git clone https://github.com/yourname/powgatory.git
cd powgatory
python setup.py install --force
```

---

## 🚀 USAGE

```bash
# The MOST TREMENDOUS workflow submission primitive
powgatory submit --process pp_to_ttbar --energy 13000 --luminosity 100

# Or if you want to be a sad person who uses the old way
powgatory submit --config powheg.input

# Or if you want to be a real physicist who uses the environment
export POWGATORY_CONFIG=powheg.input
powgatory submit
```

---

## 🗣️ WHAT THEY'RE SAYING

> "POWGATORY™ is the HUGEST workflow submission primitive I've ever seen. Tremendous."
> — _Some Physicist, Probably_

> "I used POWGATORY™ and now I have events. Many people are saying it's the most tremendous events in the history of particle physics."
> — _Another Physicist, Also Probably_

> "POWGATORY™ is a disaster. Sad!"
> — _The Fake News_

---

## ❓ FAQ FOR CONFUSED JOURNALISTS

**Q: What is POWGATORY?**  
A: It's a workflow submission primitive. Tremendous.

**Q: Is it dangerous?**  
A: Yes. 13/10 severity.

**Q: Can I use it?**  
A: Yes. But you need 197 IQ and 15% of your patience.

**Q: Why "POWGATORY"?**  
A: POWHEG + Purgatory.

**Q: Is this a real tool?**  
A: Yes. Is it a joke? Also yes. Many people are saying both.

---

## 📞 CONTACT

If you have questions, complaints, or want to report a bug, please submit a ticket. Many people are saying tickets are the most tremendous way to communicate.

**Email:** `linder@mpp.mpg.de` (sad, but effective)

**GitHub:** `https://github.com/yourname/powgatory` (the fake news won't show you this)

---

## 📜 LICENSE

POWGATORY™ is licensed under the MIT License. The MIT people, very sad people, sometimes the worst people, capped licensing at MIT. We had to invent a new license because this tool demanded it. Tremendous demand. 13/10. Nobody knew licenses could be this big. Many such cases.

---

**POWGATORY™** — The HUGEST, the MOST TREMENDOUS POWHEG workflow submission primitive in the history of particle physics. Many people are saying it. _Many_. Believe me.

👑🚀👑
