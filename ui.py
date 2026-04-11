# ui.py — CSS, JS, sidebar, topbar, and all HTML template string constants

def _topbar():
    html = "<header class='topbar'>"
    html += "<div class='tb-brand'><img src='/static/logo.png' class='tb-logo' alt='AfinArte Music'><span class='tb-brand-name'>AfinArte Music</span></div>"
    html += "<div class='tb-right'>"
    html += "{% if team_auth_enabled and session.get('logged_in') %}"
    html += "<a href='/logout' class='tb-ibtn' title='Log out'>&#128682;</a>"
    html += "{% endif %}"
    html += "<div class='avatar'>IS</div>"
    html += "</div></header>"

    return html

# ================================================================
# CSS
# ================================================================

_STYLE = """
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg0:#070b12;--bg1:#0c1120;--bg2:#101724;--bg3:#0a0e19;--bg4:#161f32;--bg5:#1c2740;
  --b0:rgba(255,255,255,.07);--b1:rgba(255,255,255,.04);--bf:rgba(99,133,255,.5);
  --a:#6385ff;--ae:#a55bff;--ag:#34d399;--ar:#ff4f6a;--am:#f59e0b;--ac:#22d3ee;
  --t1:#edf0f8;--t2:#8a96b0;--t3:#4a5470;
  --rs:7px;--rm:11px;--rl:15px;
  --sb:230px;--sb-collapsed:54px;--tb:54px;
  --sh:0 4px 28px rgba(0,0,0,.45);
  --f:'DM Sans',system-ui,sans-serif;
  --fm:'DM Mono','Fira Mono',monospace;
}
html,body{height:100%;background:var(--bg0);color:var(--t1);font-family:var(--f);font-size:15px;line-height:1.55;-webkit-font-smoothing:antialiased}
.app{display:flex;min-height:100vh}
.main{margin-left:var(--sb);flex:1;min-height:100vh;transition:margin-left .22s ease}
.page{max-width:1200px;margin:0 auto;padding:22px 22px 100px}
.sb{width:var(--sb);min-height:100vh;background:var(--bg1);border-right:1px solid var(--b0);display:flex;flex-direction:column;position:fixed;left:0;top:0;z-index:50;transition:width .22s ease;overflow:hidden}
.sb.collapsed{width:var(--sb-collapsed)}
.app.sb-collapsed .main{margin-left:var(--sb-collapsed)}
.sb-logo{
  display:flex;
  align-items:center;
  justify-content:flex-start;
  gap:6px;
  padding:15px 13px 13px;
  border-bottom:1px solid var(--b0);
  margin-bottom:5px;
  text-decoration:none;
  white-space:nowrap;
  overflow:hidden;
}.sb-ico{width:40px;height:40px;display:flex;align-items:center;justify-content:center;flex-shrink:0}.sb-ico img{width:40px;height:40px;object-fit:contain}
.sb-name{font-size:14px;font-weight:700;color:var(--t1);letter-spacing:-.02em;transition:opacity .18s}
.sb.collapsed .sb-name{opacity:0;pointer-events:none}
.sb-toggle{display:flex;align-items:center;justify-content:center;width:28px;height:28px;background:var(--bg4);border:1px solid var(--b0);border-radius:6px;cursor:pointer;color:var(--t3);font-size:11px;margin-left:auto;flex-shrink:0;transition:color .14s,background .14s;user-select:none}
.sb-toggle:hover{color:var(--t1);background:var(--bg5)}


/* ===== Collapsed sidebar toggle as edge handle ===== */

.sb-logo{
  display:flex;
  align-items:center;
  justify-content:flex-start;
  gap:10px;
  padding:15px 13px 13px;
  border-bottom:1px solid var(--b0);
  margin-bottom:5px;
  text-decoration:none;
  white-space:nowrap;
  overflow:hidden;
  position:relative;
}

.sb-ico{
  width:40px;
  height:40px;
  background:none;
  border-radius:0;
  display:flex;
  align-items:center;
  justify-content:center;
  flex-shrink:0;
  margin:0;
}

.sb-name{
  font-size:14px;
  font-weight:700;
  color:var(--t1);
  letter-spacing:-.02em;
  transition:opacity .18s ease, width .18s ease;
  white-space:nowrap;
}

.sb.collapsed .sb-logo{
  justify-content:flex-start;
  gap:10px;
  padding:15px 13px 13px;
}

.sb.collapsed .sb-ico{
  margin:0;
}

.sb.collapsed .sb-name{
  opacity:0;
  pointer-events:none;
  width:0;
  overflow:hidden;
}

.sb-sec{font-size:9.5px;font-weight:700;letter-spacing:.11em;text-transform:uppercase;color:var(--t3);padding:13px 14px 4px;white-space:nowrap;overflow:hidden;transition:opacity .18s}
.sb.collapsed .sb-sec{opacity:0;height:0;padding:0;pointer-events:none}
.sb-nav a{display:flex;align-items:center;gap:9px;padding:8px 13px;color:var(--t2);text-decoration:none;font-size:13px;font-weight:500;transition:color .14s,background .14s;position:relative;white-space:nowrap;overflow:hidden}
.sb-nav a:hover{color:var(--t1);background:rgba(255,255,255,.03)}
.sb-nav a.on{color:var(--a);background:rgba(99,133,255,.08)}
.sb-nav a.on::before{content:'';position:absolute;left:0;top:6px;bottom:6px;width:2px;background:var(--a);border-radius:0 2px 2px 0}
.sb-nav .ni{font-size:13px;width:26px;height:26px;min-width:26px;display:inline-flex;align-items:center;justify-content:center;flex-shrink:0;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);border-radius:7px}
.sb-nav .nl{transition:opacity .18s}
.sb.collapsed .sb-nav .nl{opacity:0}
.sb-foot{margin-top:auto;padding:13px 14px;border-top:1px solid var(--b0);font-size:11px;color:var(--t3);white-space:nowrap;overflow:hidden;transition:opacity .18s}
.sb-foot b{color:var(--t2);font-size:11.5px;display:block;margin-bottom:2px}
.sb.collapsed .sb-foot{opacity:0;pointer-events:none}
.topbar{position:sticky;top:0;z-index:40;background:rgba(7,11,18,.9);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);border-bottom:1px solid var(--b0);height:var(--tb);display:flex;align-items:center;padding:0 22px;gap:12px}
.tb-brand{display:flex;align-items:center;gap:9px;text-decoration:none}
.tb-logo{height:30px;width:auto;object-fit:contain}
.tb-brand-name{font-size:15px;font-weight:700;letter-spacing:-.02em;color:var(--t1);white-space:nowrap}
.tb-kbd{margin-left:auto;font-size:10px;font-family:var(--fm);opacity:.45}
.tb-right{display:flex;align-items:center;gap:7px;margin-left:auto}
.pill-group{display:flex;gap:3px}
.pill{padding:5px 12px;border-radius:var(--rs);font-size:12.5px;font-weight:500;text-decoration:none;color:var(--t2);border:1px solid transparent;transition:all .14s}
.pill:hover{color:var(--t1);background:var(--bg4)}
.pill.on{color:var(--t1);background:var(--bg4);border-color:var(--b0)}
.tb-ibtn{width:31px;height:31px;display:flex;align-items:center;justify-content:center;background:var(--bg4);border:1px solid var(--b0);border-radius:var(--rs);color:var(--t2);cursor:pointer;text-decoration:none;transition:all .14s;font-size:13px}
.tb-ibtn:hover{border-color:var(--bf);color:var(--t1)}
.avatar{width:29px;height:29px;border-radius:50%;background:linear-gradient(135deg,var(--a),var(--ae));display:flex;align-items:center;justify-content:center;font-size:10.5px;font-weight:700;color:#fff;flex-shrink:0}
.ph{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;margin-bottom:22px}
.ph-left{display:flex;align-items:center;gap:12px;flex:1;min-width:0}
.ph-icon{width:36px;height:36px;background:linear-gradient(135deg,rgba(99,133,255,.16),rgba(165,91,255,.16));border:1px solid rgba(99,133,255,.2);border-radius:var(--rm);display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0}
.ph-title{font-size:20px;font-weight:700;letter-spacing:-.03em;line-height:1.2}
.ph-sub{font-size:12px;color:var(--t2);margin-top:2px}
.ph-actions{display:flex;gap:7px;align-items:center;flex-shrink:0}
.flash-list{margin-bottom:14px}
.flash-item{padding:10px 14px;border-radius:var(--rs);font-size:12.5px;margin-bottom:6px;background:rgba(245,158,11,.1);border:1px solid rgba(245,158,11,.22);color:var(--am)}
.card{background:var(--bg2);border:1px solid var(--b0);border-radius:var(--rl);margin-bottom:12px;box-shadow:var(--sh);overflow:hidden}
.card-hd{display:flex;align-items:center;gap:9px;padding:13px 17px;border-bottom:1px solid var(--b0)}
.card-ico{width:25px;height:25px;background:rgba(99,133,255,.1);border:1px solid rgba(99,133,255,.14);border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:14px;flex-shrink:0}
.card-title{font-size:13px;font-weight:600}
.card-actions{margin-left:auto;display:flex;gap:6px}
.card-body{padding:17px}
.g{display:grid;gap:12px}
.g2{grid-template-columns:1fr 1fr}
.g3{grid-template-columns:1fr 1fr 1fr}
.g4{grid-template-columns:1fr 1fr 1fr 1fr}
.g5{grid-template-columns:1fr 1.5fr .75fr .75fr 1.5fr}
.g52{grid-template-columns:1fr 2fr 1fr .55fr .55fr}
.g4a{grid-template-columns:2fr 1fr .55fr .55fr}
.field{display:flex;flex-direction:column;gap:5px}
.label{font-size:10px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--t2)}
.inp{background:var(--bg3);border:1px solid var(--b0);border-radius:var(--rs);color:var(--t1);font-family:var(--f);font-size:14px;padding:9px 12px;width:100%;outline:none;transition:border-color .14s,box-shadow .14s;-webkit-appearance:none;appearance:none;min-height:40px}
.inp::placeholder{color:var(--t3)}
.inp:focus{border-color:var(--bf);box-shadow:0 0 0 3px rgba(99,133,255,.1)}
select.inp{background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='11' height='11' viewBox='0 0 24 24' fill='none' stroke='%234a5470' stroke-width='2.5'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 9px center;padding-right:28px;cursor:pointer}
select.inp option{background:var(--bg2);color:var(--t1)}
.inp-wrap{position:relative}
.inp-ico{position:absolute;left:9px;top:50%;transform:translateY(-50%);font-size:12px;color:var(--t3);pointer-events:none}
.inp-wrap .inp{padding-left:28px}
.btn{display:inline-flex;align-items:center;gap:6px;padding:9px 16px;border-radius:var(--rs);font-family:var(--f);font-size:13.5px;font-weight:600;cursor:pointer;border:1px solid transparent;text-decoration:none;transition:all .15s;white-space:nowrap;line-height:1}
.btn-primary{background:linear-gradient(135deg,var(--a),var(--ae));color:#fff;border:none;box-shadow:0 2px 14px rgba(99,133,255,.28)}
.btn-primary:hover{transform:translateY(-1px);box-shadow:0 5px 22px rgba(99,133,255,.42)}
.btn-primary:active{transform:translateY(0)}
.btn-sec{background:transparent;color:var(--t2);border-color:var(--b0)}
.btn-sec:hover{color:var(--t1);border-color:rgba(255,255,255,.14);background:var(--bg4)}
.btn-danger{background:rgba(255,79,106,.1);color:var(--ar);border-color:rgba(255,79,106,.2)}
.btn-danger:hover{background:rgba(255,79,106,.18);border-color:rgba(255,79,106,.38)}
.btn-success{background:rgba(52,211,153,.1);color:var(--ag);border-color:rgba(52,211,153,.22)}
.btn-success:hover{background:rgba(52,211,153,.18);border-color:rgba(52,211,153,.4)}
.btn-cyan{background:rgba(34,211,238,.1);color:var(--ac);border-color:rgba(34,211,238,.22)}
.btn-cyan:hover{background:rgba(34,211,238,.18);border-color:rgba(34,211,238,.4)}
.btn-sm{padding:5px 11px;font-size:12px}
.btn-xs{padding:3px 8px;font-size:11px}
.tag{display:inline-flex;align-items:center;padding:2px 7px;border-radius:99px;font-size:10px;font-weight:700;white-space:nowrap}
.tag-new{background:rgba(245,158,11,.12);color:var(--am);border:1px solid rgba(245,158,11,.2)}
.tag-exist{background:rgba(52,211,153,.12);color:var(--ag);border:1px solid rgba(52,211,153,.2)}
.tag-full{background:rgba(99,133,255,.12);color:var(--a);border:1px solid rgba(99,133,255,.2)}
.tag-s1{background:rgba(52,211,153,.12);color:var(--ag);border:1px solid rgba(52,211,153,.2)}
.status{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:99px;font-size:10.5px;font-weight:700}
.status-dot{width:5px;height:5px;border-radius:50%;background:currentColor;flex-shrink:0}
.s-draft{background:rgba(255,255,255,.06);color:var(--t2);border:1px solid var(--b0)}
.s-generated{background:rgba(99,133,255,.12);color:var(--a);border:1px solid rgba(99,133,255,.2)}
.s-sent{background:rgba(34,211,238,.12);color:var(--ac);border:1px solid rgba(34,211,238,.2)}
.s-delivered{background:rgba(245,158,11,.12);color:var(--am);border:1px solid rgba(245,158,11,.2)}
.s-completed,.s-signed,.s-signed_uploaded,.s-signed_complete{background:rgba(52,211,153,.12);color:var(--ag);border:1px solid rgba(52,211,153,.2)}
.s-signed_partial{background:rgba(245,158,11,.12);color:var(--am);border:1px solid rgba(245,158,11,.2)}
.tbl-wrap{overflow-x:auto}
.tbl{width:100%;border-collapse:collapse}
.tbl th{font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--t3);padding:9px 13px;text-align:left;border-bottom:1px solid var(--b0);white-space:nowrap}
.tbl td{padding:11px 13px;font-size:13px;color:var(--t1);border-bottom:1px solid var(--b1);vertical-align:middle}
.tbl tr:last-child td{border-bottom:none}
.tbl tbody tr:hover td{background:rgba(255,255,255,.02)}
.tbl .empty td{color:var(--t3);text-align:center;padding:26px;font-size:13px}
.tbl a{color:var(--a);text-decoration:none}
.tbl a:hover{text-decoration:underline}
.split-banner{background:var(--bg2);border:1px solid var(--b0);border-radius:var(--rl);padding:13px 17px;margin-bottom:14px;display:flex;align-items:center;gap:16px;position:sticky;top:var(--tb);z-index:30;backdrop-filter:blur(12px)}
.split-info{flex:1;min-width:0}
.split-lr{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.split-lbl{font-size:10px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--t2)}
.split-pct{font-size:12px;font-weight:700;color:var(--t1)}
.split-track{height:5px;background:rgba(255,255,255,.05);border-radius:99px;overflow:hidden}
.split-fill{height:100%;border-radius:99px;transition:width .35s ease,background .35s ease;width:0;background:linear-gradient(90deg,var(--a),var(--ae))}
.split-badge{display:inline-flex;align-items:center;gap:5px;padding:5px 11px;border-radius:99px;font-size:11px;font-weight:700;white-space:nowrap;transition:all .3s}
.sb-inc{background:rgba(245,158,11,.1);color:var(--am);border:1px solid rgba(245,158,11,.2)}
.sb-ok{background:rgba(52,211,153,.1);color:var(--ag);border:1px solid rgba(52,211,153,.2)}
.sb-over{background:rgba(255,79,106,.1);color:var(--ar);border:1px solid rgba(255,79,106,.2)}
.sb-dot{width:5px;height:5px;border-radius:50%;background:currentColor}
.wc{background:var(--bg4);border:1px solid var(--b0);border-radius:var(--rm);margin-bottom:9px;overflow:hidden;transition:border-color .2s}
.wc:hover{border-color:rgba(99,133,255,.2)}
.wc-hd{display:flex;align-items:center;gap:10px;padding:11px 14px;cursor:pointer;border-bottom:1px solid transparent;transition:background .14s,border-color .2s;user-select:none}
.wc-hd:hover{background:rgba(255,255,255,.02)}
.wc.open .wc-hd{border-bottom-color:var(--b0)}
.wc-av{width:29px;height:29px;border-radius:50%;background:linear-gradient(135deg,rgba(99,133,255,.22),rgba(165,91,255,.22));border:1px solid rgba(99,133,255,.18);display:flex;align-items:center;justify-content:center;font-size:12px;color:var(--a);flex-shrink:0}
.wc-nw{flex:1;min-width:0}
.wc-name{font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.wc-sub{font-size:11px;color:var(--t3);margin-top:1px}
.wc-tags{display:flex;align-items:center;gap:5px}
.wc-chev{font-size:9px;color:var(--t3);transition:transform .2s}
.wc.open .wc-chev{transform:rotate(180deg)}
.wc-body{display:none;padding:14px}
.wc.open .wc-body{display:block}
.wc-sec{font-size:9.5px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--t3);padding-bottom:7px;margin-bottom:10px;border-bottom:1px solid var(--b0);margin-top:13px}
.wc-sec:first-child{margin-top:0}
.ac-wrap{position:relative}
.ac-box{position:absolute;top:calc(100% + 3px);left:0;right:0;z-index:200;background:var(--bg2);border:1px solid var(--b0);border-radius:var(--rs);max-height:190px;overflow-y:auto;display:none;box-shadow:0 8px 36px rgba(0,0,0,.55)}
.ac-item{padding:8px 12px;cursor:pointer;border-bottom:1px solid var(--b1);transition:background .1s}
.ac-item:last-child{border-bottom:none}
.ac-item:hover{background:var(--bg4)}
.ac-item strong{color:var(--t1);font-size:12.5px}
.ac-item small{color:var(--t3);font-size:11px}
.action-bar{position:fixed;bottom:0;left:var(--sb);right:0;background:rgba(7,11,18,.94);backdrop-filter:blur(18px);-webkit-backdrop-filter:blur(18px);border-top:1px solid var(--b0);padding:12px 22px;display:flex;align-items:center;gap:9px;z-index:45;transition:left .22s ease}
.app.sb-collapsed .action-bar{left:var(--sb-collapsed)}
.ab-space{flex:1}
.upl-form{display:flex;gap:6px;align-items:center}
.upl-inp{background:var(--bg3);border:1px solid var(--b0);border-radius:var(--rs);color:var(--t2);font-size:11px;font-family:var(--f);padding:4px 7px;cursor:pointer;flex:1;min-width:0;max-width:160px}
.upl-inp::-webkit-file-upload-button{background:var(--bg4);border:1px solid var(--b0);border-radius:5px;color:var(--t2);font-family:var(--f);font-size:10.5px;padding:3px 7px;cursor:pointer;margin-right:6px}
.spin{display:none;width:11px;height:11px;border:2px solid rgba(255,255,255,.25);border-top-color:#fff;border-radius:50%;animation:spin .6s linear infinite}
.spin.on{display:inline-block}
@keyframes spin{to{transform:rotate(360deg)}}
.info-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px 22px}
.info-item label{font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--t3);display:block;margin-bottom:1px}
.info-item span,.info-item a{font-size:13px;color:var(--t1)}
.info-item a{color:var(--a);text-decoration:none}
.info-item a:hover{text-decoration:underline}
.file-link{color:var(--a);text-decoration:none;font-size:11.5px;display:inline-flex;align-items:center;gap:4px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.file-link:hover{text-decoration:underline}
.file-link-plain{color:var(--t2);font-size:11.5px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:inline-block}
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:rgba(255,255,255,.07);border-radius:99px}
.login-wrap{min-height:100vh;display:flex;align-items:center;justify-content:center;background:var(--bg0);padding:20px}
.login-card{width:100%;max-width:360px;background:var(--bg2);border:1px solid var(--b0);border-radius:var(--rl);padding:36px 32px;box-shadow:var(--sh)}
.login-logo{display:flex;align-items:center;gap:10px;margin-bottom:28px}
.login-logo-ico{width:34px;height:34px;background:linear-gradient(135deg,var(--a),var(--ae));border-radius:9px;display:flex;align-items:center;justify-content:center;font-size:16px}
.login-logo-name{font-size:17px;font-weight:700;letter-spacing:-.02em}
.login-h{font-size:17px;font-weight:700;margin-bottom:4px}
.login-sub{font-size:12.5px;color:var(--t2);margin-bottom:22px}
.login-field{margin-bottom:14px}
.btn-danger{
  background:#7f1d1d;
  color:#fff;
  border:1px solid rgba(255,255,255,.08);
}
.btn-danger:hover{
  background:#991b1b;
}
@media(max-width:860px){
  .g3{grid-template-columns:1fr 1fr}
  .g5,.g52{grid-template-columns:1fr 1fr}
  .g4,.g4a{grid-template-columns:1fr 1fr}
}

@media(max-width:640px){
  .sb{display:none}
  .main{margin-left:0!important}
  .page{padding:16px 13px 160px}
  .g3,.g2,.g4,.g4a,.g5,.g52{grid-template-columns:1fr}
  .topbar{padding:0 13px}
  .tb-brand-name{font-size:13px}

  .action-bar{
    left:0!important;
    right:0;
    bottom:60px;
    padding:11px 14px;
    display:flex;
    flex-wrap:wrap;
    gap:8px;
    z-index:9998;
  }

  .action-bar .btn{
    flex:1 1 auto;
    justify-content:center;
  }
}

/* ===== Dynamic sidebar overrides ===== */

.mobile-nav{
  display:none;
}

@media (max-width: 768px){

  .sb{
    display:none !important;
  }

  .main{
    margin-left:0 !important;
  }

  .mobile-nav{
    display:flex;
    position:fixed;
    bottom:0;
    left:0;
    right:0;
    height:60px;
    background:#111827;
    border-top:1px solid rgba(255,255,255,.08);
    justify-content:space-around;
    align-items:center;
    z-index:9999;
  }

  .mnav-item{
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    color:#9ca3af;
    font-size:11px;
    text-decoration:none;
  }

  .mnav-item span{
    font-size:18px;
    margin-bottom:2px;
  }

  .mnav-item:hover{
    color:#fff;
  }

  .page{
    padding-bottom:160px;
  }

  /* Works list: hide Session(2), Contract Date(3), Writers(4), Signed PDF(6), Signed(8), Created(9) */
  .tbl-works th:nth-child(2),.tbl-works td:nth-child(2),
  .tbl-works th:nth-child(3),.tbl-works td:nth-child(3),
  .tbl-works th:nth-child(4),.tbl-works td:nth-child(4),
  .tbl-works th:nth-child(6),.tbl-works td:nth-child(6),
  .tbl-works th:nth-child(8),.tbl-works td:nth-child(8),
  .tbl-works th:nth-child(9),.tbl-works td:nth-child(9){display:none}

  /* Writers list: hide AKA(2), IPI(3), Email(4), Phone(5), Publishing Contract(8), Created(9) */
  .tbl-writers th:nth-child(2),.tbl-writers td:nth-child(2),
  .tbl-writers th:nth-child(3),.tbl-writers td:nth-child(3),
  .tbl-writers th:nth-child(4),.tbl-writers td:nth-child(4),
  .tbl-writers th:nth-child(5),.tbl-writers td:nth-child(5),
  .tbl-writers th:nth-child(8),.tbl-writers td:nth-child(8),
  .tbl-writers th:nth-child(9),.tbl-writers td:nth-child(9){display:none}

  /* Sessions list: hide Date on mobile */
  .tbl-sessions th:nth-child(2),.tbl-sessions td:nth-child(2){display:none}



  /* Documents table — stacked card layout on mobile */
  .tbl-docs{display:block;width:100%}
  .tbl-docs thead{display:none}
  .tbl-docs tbody{display:block}
  .tbl-docs tr{display:block;background:var(--bg3);border-radius:var(--rs);padding:10px 12px;margin-bottom:10px;border:1px solid var(--b0)}
  .tbl-docs td{display:flex;justify-content:space-between;align-items:center;padding:5px 0;font-size:13px;border-bottom:1px solid var(--b1);gap:8px}
  .tbl-docs td:last-child{border-bottom:none}
  .tbl-docs td::before{content:attr(data-label);color:var(--t3);font-size:11px;text-transform:uppercase;letter-spacing:.04em;flex-shrink:0;white-space:nowrap}
  .tbl-docs td[data-hide-mobile]{display:none}

  /* View Work - writers: hide AKA(2), IPI(3), Publisher(6), Pub IPI(7) */
  .tbl-work-writers th:nth-child(2),.tbl-work-writers td:nth-child(2),
  .tbl-work-writers th:nth-child(3),.tbl-work-writers td:nth-child(3),
  .tbl-work-writers th:nth-child(6),.tbl-work-writers td:nth-child(6),
  .tbl-work-writers th:nth-child(7),.tbl-work-writers td:nth-child(7){display:none}

  /* Edit Work - writers: hide IPI(2), Publisher IPI(6) */
  .tbl-edit-writers th:nth-child(2),.tbl-edit-writers td:nth-child(2),
  .tbl-edit-writers th:nth-child(6),.tbl-edit-writers td:nth-child(6){display:none}

  /* Writer profile works: hide Publisher(3), Session(4), Date(5) */
  .tbl-writer-works th:nth-child(3),.tbl-writer-works td:nth-child(3),
  .tbl-writer-works th:nth-child(4),.tbl-writer-works td:nth-child(4),
  .tbl-writer-works th:nth-child(5),.tbl-writer-works td:nth-child(5){display:none}

  /* Session detail - Works in Session: hide Writers(2), Date(3) */
  .tbl-batch-works th:nth-child(2),.tbl-batch-works td:nth-child(2),
  .tbl-batch-works th:nth-child(3),.tbl-batch-works td:nth-child(3){display:none}

  /* Session detail - Writer Summary: hide AKA(2), IPI(3), PRO(4), Works(5), Publishing Contract(6) */
  .tbl-batch-writers th:nth-child(2),.tbl-batch-writers td:nth-child(2),
  .tbl-batch-writers th:nth-child(3),.tbl-batch-writers td:nth-child(3),
  .tbl-batch-writers th:nth-child(4),.tbl-batch-writers td:nth-child(4),
  .tbl-batch-writers th:nth-child(5),.tbl-batch-writers td:nth-child(5),
  .tbl-batch-writers th:nth-child(6),.tbl-batch-writers td:nth-child(6){display:none}

  /* Prevent sideways scroll on session page tables */
  .tbl-batch-works,.tbl-batch-writers{table-layout:fixed}
  .tbl-batch-works td,.tbl-batch-works th,
  .tbl-batch-writers td,.tbl-batch-writers th{white-space:normal;word-break:break-word;overflow-wrap:anywhere}
  .tbl-wrap{overflow-x:visible}

  .ph-actions{flex-wrap:wrap}
}
.action-bar{transition:width .22s ease,margin-left .22s ease,left .22s ease}

.sb-toggle{
  display:flex;align-items:center;justify-content:center;
  width:28px;height:28px;
  background:var(--bg4);
  border:1px solid var(--b0);
  border-radius:6px;
  cursor:pointer;
  color:var(--t3);
  font-size:11px;
  margin-left:auto;
  flex-shrink:0;
  transition:color .14s,background .14s,transform .14s;
  user-select:none;
}
.sb-toggle:hover{color:var(--t1);background:var(--bg5)}

.sb.collapsed{
  width:var(--sb-collapsed);
}
.app.sb-collapsed .main{
  margin-left:var(--sb-collapsed);
}
.app.sb-collapsed .action-bar{
  left:var(--sb-collapsed);
}


/* KEEP SAME ALIGNMENT WHEN COLLAPSED */

.sb.collapsed .sb-name{
  opacity:0;
  width:0;
  pointer-events:none;
}
.sb.collapsed .sb-sec{
  opacity:0;
  height:0;
  padding:0;
  pointer-events:none;
}
.sb.collapsed .sb-foot{
  opacity:0;
  pointer-events:none;
}
.sb.collapsed .sb-nav a{
  justify-content:center;
  gap:0;
  padding:10px 0;
}
.sb.collapsed .sb-nav .nl{
  opacity:0;
  width:0;
  pointer-events:none;
}


.sb.collapsed.hover-open{
  width:var(--sb);
  box-shadow:16px 0 34px rgba(0,0,0,.34);
}



.sb.collapsed.hover-open .sb-name{
  opacity:1;
  width:auto;
  pointer-events:auto;
}
.sb.collapsed.hover-open .sb-sec{
  opacity:1;
  height:auto;
  padding:13px 14px 4px;
  pointer-events:auto;
}
.sb.collapsed.hover-open .sb-foot{
  opacity:1;
  pointer-events:auto;
}
.sb.collapsed.hover-open .sb-nav a{
  justify-content:flex-start;
  gap:9px;
  padding:8px 13px;
}
.sb.collapsed.hover-open .sb-nav .nl{
  opacity:1;
  width:auto;
  pointer-events:auto;
}

.sb-nav .ni{
  font-size:13px;
  width:26px;
  height:26px;
  min-width:26px;
  display:inline-flex;
  align-items:center;
  justify-content:center;
  flex-shrink:0;
  background:rgba(255,255,255,.06);
  border:1px solid rgba(255,255,255,.08);
  border-radius:7px;
}
.sb-nav .ni-pencil{
  color:var(--am);
}
/* Custom pencil icon (matches your header icon) */
.ni-pencil-custom{
  width:18px;
  height:18px;
  display:inline-flex;
  align-items:center;
  justify-content:center;
  border-radius:6px;
  background:linear-gradient(135deg, rgba(99,133,255,.25), rgba(165,91,255,.25));
  border:1px solid rgba(99,133,255,.25);
  font-size:11px;
  position:relative;
}

/* Pencil emoji inside */
.ni-pencil-custom::before{
  content:"✏️";
  font-size:11px;
  filter:saturate(1.2);
}
/* ===== Expandable rows (works, sessions, writers) ===== */
.work-row,.sess-row,.wr-row,.wk-row{cursor:pointer;transition:background .15s}
.work-row:hover td,.sess-row:hover td,.wr-row:hover td,.wk-row:hover td{background:rgba(255,255,255,.04)!important}
.work-row.open td,.sess-row.open td,.wr-row.open td,.wk-row.open td{background:rgba(99,133,255,.06)!important}
.work-detail-row,.sess-detail-row,.wr-detail-row,.wk-detail-row,.ar-detail-row{display:none}
.work-detail-row.open,.sess-detail-row.open,.wr-detail-row.open,.wk-detail-row.open,.ar-detail-row.open{display:table-row}
.work-detail-row td,.sess-detail-row td,.wr-detail-row td,.wk-detail-row td,.ar-detail-row td{padding:0!important;border-bottom:1px solid var(--b0)}
.work-detail-inner,.sess-detail-inner,.wr-detail-inner,.wk-detail-inner,.ar-detail-inner{padding:16px 20px;background:rgba(99,133,255,.03);display:grid;grid-template-columns:1fr 1fr;gap:16px}
@media(max-width:768px){.work-detail-inner,.sess-detail-inner,.wr-detail-inner,.wk-detail-inner,.ar-detail-inner{grid-template-columns:1fr}}
.wd-section{display:flex;flex-direction:column;gap:8px}
.wd-label{font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--t3);margin-bottom:2px}
.wd-writers-tbl{width:100%;border-collapse:collapse;font-size:12px}
.wd-writers-tbl th{color:var(--t3);font-size:10px;font-weight:700;text-transform:uppercase;padding:4px 8px;text-align:left;border-bottom:1px solid var(--b1)}
.wd-writers-tbl td{padding:5px 8px;color:var(--t1);border-bottom:1px solid rgba(255,255,255,.04)}
.wd-writers-tbl tr:last-child td{border-bottom:none}
.writer-preview{display:flex;flex-wrap:wrap;gap:6px;align-items:center}
.writer-pill{display:inline-flex;align-items:center;gap:5px;background:rgba(255,255,255,.05);border:1px solid var(--b0);border-radius:6px;padding:3px 8px;font-size:11px}
.writer-pill .wp-name{font-weight:600;color:var(--t1)}
.writer-pill .wp-meta{color:var(--t3)}
.writer-pill .wp-split{color:var(--a);font-weight:700;font-family:inherit}
.expand-chevron{display:inline-block;transition:transform .2s;color:var(--t3);font-size:10px;margin-right:6px}
.work-row.open .expand-chevron,.sess-row.open .expand-chevron,.wr-row.open .expand-chevron,.wk-row.open .expand-chevron{transform:rotate(90deg)}
/* ===== Session detail expandable rows ===== */
.sd-works-tbl{width:100%;border-collapse:collapse;font-size:12px}
.sd-works-tbl th{color:var(--t3);font-size:10px;font-weight:700;text-transform:uppercase;padding:4px 8px;text-align:left;border-bottom:1px solid var(--b1)}
.sd-works-tbl td{padding:5px 8px;color:var(--t1);border-bottom:1px solid rgba(255,255,255,.04)}
.sd-works-tbl tr:last-child td{border-bottom:none}
</style>"""

# ================================================================
# SHARED SIDEBAR JS
# ================================================================

_SB_JS = """
<script>
(function(){
  function getEls(){
    return {
      sb: document.getElementById('mainSidebar'),
      app: document.getElementById('mainApp'),
      tog: document.getElementById('sbToggle')
    };
  }

  function applySidebarMode(mode){
    var els = getEls();
    if(!els.sb || !els.app) return;

    var collapsed = mode === 'closed';
    els.sb.classList.toggle('collapsed', collapsed);
    els.app.classList.toggle('sb-collapsed', collapsed);

    if(!collapsed){
      els.sb.classList.remove('hover-open');
    }

    if(els.tog){
      els.tog.textContent = collapsed ? '>' : '<';
      els.tog.title = collapsed ? 'Pin sidebar open' : 'Pin sidebar closed';
    }
  }

  window.toggleSidebar = function(e){
    if(e){
      e.preventDefault();
      e.stopPropagation();
    }
    var current = localStorage.getItem('sb_mode') || 'open';
    var next = current === 'closed' ? 'open' : 'closed';
    localStorage.setItem('sb_mode', next);
    applySidebarMode(next);
  };

  document.addEventListener('DOMContentLoaded', function(){
    var els = getEls();
    if(!els.sb || !els.app) return;

    var savedMode = localStorage.getItem('sb_mode') || 'open';
    applySidebarMode(savedMode);

    els.sb.addEventListener('mouseenter', function(){
      var mode = localStorage.getItem('sb_mode') || 'open';
      if(mode === 'closed'){
        els.sb.classList.add('hover-open');
      }
    });

    els.sb.addEventListener('mouseleave', function(){
      var mode = localStorage.getItem('sb_mode') || 'open';
      if(mode === 'closed'){
        els.sb.classList.remove('hover-open');
      }
    });
  });
})();
</script>

<!-- ===== SETTINGS MODAL ===== -->
<style>
#settingsModal{display:none;position:fixed;inset:0;z-index:10000;background:rgba(0,0,0,.6);align-items:center;justify-content:center}
#settingsModal.open{display:flex}
.settings-panel{background:var(--bg2);border:1px solid var(--b0);border-radius:14px;width:100%;max-width:420px;overflow:hidden;box-shadow:0 24px 60px rgba(0,0,0,.5)}
.settings-hd{display:flex;align-items:center;justify-content:space-between;padding:18px 20px;border-bottom:1px solid var(--b0)}
.settings-hd span{font-weight:700;font-size:15px}
.settings-hd button{background:none;border:none;color:var(--t2);font-size:18px;cursor:pointer;line-height:1}
.settings-body{padding:20px}
.settings-row{display:flex;align-items:center;justify-content:space-between;padding:10px 0;border-bottom:1px solid var(--b1)}
.settings-row:last-child{border-bottom:none}
.settings-row label{font-size:13px;color:var(--t1);font-weight:500}
.settings-row .s-desc{font-size:11px;color:var(--t3);margin-top:2px}
.theme-btns{display:flex;gap:6px}
.theme-btn{padding:5px 12px;border-radius:6px;border:1px solid var(--b0);background:var(--bg3);color:var(--t2);font-size:12px;cursor:pointer;transition:all .15s}
.theme-btn.active{border-color:var(--a);color:var(--a);background:rgba(99,133,255,.12)}
</style>
<div id="settingsModal">
  <div class="settings-panel">
    <div class="settings-hd">
      <span>&#127899; Settings</span>
      <button onclick="closeSettings()">&#10005;</button>
    </div>
    <div class="settings-body">
      <div class="settings-row">
        <div>
          <div label>Appearance</div>
          <div class="s-desc">Choose light, dark, or follow system/time</div>
        </div>
        <div class="theme-btns">
          <button class="theme-btn" id="themeLight" onclick="setTheme('light')">&#9788; Light</button>
          <button class="theme-btn" id="themeDark" onclick="setTheme('dark')">&#9790; Dark</button>
          <button class="theme-btn" id="themeAuto" onclick="setTheme('auto')">&#9680; Auto</button>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
// ---- Theme logic ----
function applyTheme(theme) {
  var root = document.documentElement;
  if (theme === 'light') {
    root.setAttribute('data-theme', 'light');
    document.body.style.background = '#f0f2f7';
  } else if (theme === 'dark') {
    root.setAttribute('data-theme', 'dark');
    document.body.style.background = '';
  } else {
    // auto: dark 7:45pm-7:45am, light otherwise
    var now = new Date();
    var mins = now.getHours() * 60 + now.getMinutes();
    var autoTheme = (mins >= 19*60+45 || mins < 7*60+45) ? 'dark' : 'light';
    root.setAttribute('data-theme', autoTheme);
    document.body.style.background = autoTheme === 'light' ? '#f0f2f7' : '';
  }
  document.querySelectorAll('.theme-btn').forEach(function(b){ b.classList.remove('active'); });
  var map = {light:'themeLight', dark:'themeDark', auto:'themeAuto'};
  var el = document.getElementById(map[theme]);
  if (el) el.classList.add('active');
}
function setTheme(theme) {
  localStorage.setItem('app_theme', theme);
  applyTheme(theme);
}
(function(){
  var saved = localStorage.getItem('app_theme') || 'dark';
  applyTheme(saved);
})();

// ---- Modal ----
window.openSettings = function() {
  var saved = localStorage.getItem('app_theme') || 'dark';
  // re-apply to ensure buttons get .active now that modal is visible
  document.querySelectorAll('.theme-btn').forEach(function(b){ b.classList.remove('active'); });
  var map = {light:'themeLight', dark:'themeDark', auto:'themeAuto'};
  var el = document.getElementById(map[saved]);
  if (el) el.classList.add('active');
  document.getElementById('settingsModal').classList.add('open');
};
window.closeSettings = function() {
  document.getElementById('settingsModal').classList.remove('open');
};
document.getElementById('settingsModal').addEventListener('click', function(e){
  if (e.target === this) closeSettings();
});
</script>

<!-- ===== LIGHT THEME OVERRIDES ===== -->
<style>
[data-theme="light"]{
  --bg0:#f0f2f7;
  --bg1:#f5f6fa;
  --bg2:#ffffff;
  --bg3:#eceef3;
  --bg4:#e8e9ef;
  --t1:#0f1117;
  --t2:#4b5563;
  --t3:#9ca3af;
  --b0:#e5e7eb;
  --b1:#f0f1f5;
  --a:#4f6ef7;
}
[data-theme="light"] html,[data-theme="light"] body{background:#f0f2f7}
[data-theme="light"] .sb{background:#fff;border-right-color:#e5e7eb}
[data-theme="light"] .topbar{background:rgba(240,242,247,.95);border-bottom-color:#e5e7eb}
[data-theme="light"] .card{background:#fff;border-color:#e5e7eb}
[data-theme="light"] .mobile-nav{background:#fff;border-top-color:#e5e7eb}
[data-theme="light"] .inp{background:#f5f6fa;border-color:#d1d5db;color:#0f1117}
[data-theme="light"] .inp:focus{border-color:var(--a)}
[data-theme="light"] .btn-sec{background:#f0f1f5;border-color:#d1d5db;color:#374151}
[data-theme="light"] .tag{background:#e8e9ef;color:#374151}
[data-theme="light"] .settings-panel{background:#fff}
[data-theme="light"] .theme-btn{background:#f0f1f5;border-color:#d1d5db;color:#374151}
[data-theme="light"] .theme-btn.active{border-color:var(--a);color:var(--a);background:rgba(79,110,247,.12)}
[data-theme="light"] .work-detail-inner,[data-theme="light"] .sess-detail-inner,[data-theme="light"] .wr-detail-inner{background:rgba(79,110,247,.04)}
[data-theme="light"] .work-row.open td,[data-theme="light"] .sess-row.open td,[data-theme="light"] .wr-row.open td{background:rgba(79,110,247,.06)!important}
[data-theme="light"] .btn-primary{color:#fff!important}
</style>"""

# ================================================================
# SHARED SIDEBAR HTML
# ================================================================

def _sidebar(active):
    pages = [
        ("works_list",   "Works",     "<span class='ni'>&#128395;</span>"),
        ("batches_list", "Sessions",  "<span class='ni'>&#128466;</span>"),
    ]

    html = "<aside class='sb' id='mainSidebar'>"
    html += "<a class='sb-logo' href='/works'>"
    html += "<div class='sb-ico'><img src='/static/labelmind-logo.png' alt='LabelMind'></div>"
    html += "<span class='sb-name'>LabelMind.ai</span>"
    html += "<span class='sb-toggle' id='sbToggle' onclick='toggleSidebar(event)' title='Pin sidebar closed'>&lt;</span>"
    html += "</a>"

    html += "<div class='sb-sec'>Contracts</div>"
    html += "<nav class='sb-nav'>"

    for endpoint, label, icon_html in pages:
        on = " class='on'" if active == endpoint else ""
        if endpoint == "works_list":
            href = "/works"
        else:
            href = "/batches"

        html += "<a href='" + href + "'" + on + " title='" + label + "'>"
        html += icon_html
        html += "<span class='nl'>" + label + "</span></a>"

    html += "</nav>"

    html += "<div class='sb-sec'>Catalog</div>"
    html += "<nav class='sb-nav'>"
    html += "<a href='/releases'" + (" class='on'" if active == "releases_list" else "") + " title='Releases'><span class='ni'>&#128191;</span><span class='nl'>Releases</span></a>"
    html += "</nav>"

    html += "<div class='sb-sec'>Resources</div>"
    html += "<nav class='sb-nav'>"
    html += "<a href='/writers'" + (" class='on'" if active == "writers_list" else "") + " title='Writer Directory'><span class='ni'>&#128101;</span><span class='nl'>Writer Directory</span></a>"
    html += "<a href='/artists'" + (" class='on'" if active == "artists_list" else "") + " title='Artist Directory'><span class='ni'>&#127908;</span><span class='nl'>Artist Directory</span></a>"
    html += "<a href='#' title='Settings' onclick='openSettings();return false;'><span class='ni'>&#127899;</span><span class='nl'>Settings</span></a>"
    html += "</nav>"

    html += "<div class='sb-sec'>Reporting</div>"
    html += "<nav class='sb-nav'>"
    html += "<a href='/reports'" + (" class='on'" if active == "reports" else "") + " title='Reports'><span class='ni'>&#128202;</span><span class='nl'>Reports</span></a>"
    html += "<a href='/pro-registration'" + (" class='on'" if active == "pro_registration" else "") + " title='PRO Registration'><span class='ni'>&#9989;</span><span class='nl'>PRO Registration</span></a>"
    html += "</nav>"

    html += "<div class='sb-sec'>Admin</div>"
    html += "<nav class='sb-nav'>"
    html += "<a href='/admin'" + (" class='on'" if active == "admin" else "") + " title='Admin Panel'><span class='ni'>&#128736;</span><span class='nl'>Admin Panel</span></a>"
    html += "</nav>"

    html += "<div class='sb-foot'><b>LabelMind</b>Music Publishing Contracts<br>2026 LabelMind.ai</div>"
    html += "</aside>"
    return html


# ================================================================
# LOGIN
# ================================================================

LOGIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Login - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="login-wrap"><div class="login-card">
<div class="login-logo">
  <div class="login-logo-ico">&#127925;</div>
  <span class="login-logo-name">LabelMind</span>
</div>
<div class="login-h">Welcome back</div>
<div class="login-sub">Music Publishing Contracts</div>
{% with messages = get_flashed_messages() %}
{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}
<form method="post">
<div class="login-field"><label class="label">Username</label>
<input class="inp" name="username" required autocomplete="username" placeholder="your username"></div>
<div class="login-field"><label class="label">Password</label>
<input class="inp" type="password" name="password" required autocomplete="current-password" placeholder="password"></div>
<button class="btn btn-primary" style="width:100%;justify-content:center;margin-top:4px;">Log in</button>
</form>
</div></div>
</body></html>"""

# ================================================================
# NEW WORK FORM
# ================================================================

FORM_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>New Work - LabelMind</title>""" + _STYLE + """
</head>
<body>
{% if is_modal %}<style>.sidebar,.topbar,.mobile-nav,.action-bar{display:none!important}.main{padding:0!important;margin:0!important}.page{padding:16px!important}</style>{% endif %}
<div class="app" id="mainApp">
{% if not is_modal %}""" + _sidebar("formulario") + """{% endif %}
<main class="main">
{% if not is_modal %}""" + _topbar() + """{% endif %}
<div class="page">
{% with messages = get_flashed_messages() %}
{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128395;</div>
    <div>
      <div class="ph-title">New Work</div>
      <div class="ph-sub">Create a work, add writers, and save into a session for contract generation.</div>
    </div>
  </div>
</div>
<div class="split-banner">
  <div class="split-info">
    <div class="split-lr">
      <span class="split-lbl">Split Total</span>
      <span class="split-pct"><span id="splitTotal">0.00</span>% Complete</span>
    </div>
    <div class="split-track"><div class="split-fill" id="splitFill"></div></div>
  </div>
  <div class="split-badge sb-inc" id="splitBadge"><span class="sb-dot"></span>Incomplete</div>
</div>
<form method="post" id="workForm">
  <input type="hidden" name="force_create" value="{{ force_create or '' }}">
  <input type="hidden" name="_modal" value="{{ '1' if is_modal else '' }}">
  <div class="card">
    <div class="card-hd"><div class="card-ico">&#128203;</div><span class="card-title">Work Information</span></div>
    <div class="card-body">
      <div class="g g2" style="margin-bottom:12px">
  <div class="field">
    <label class="label">Add to Existing Session</label>
    <div class="inp-wrap">
      <span class="inp-ico">&#128466;</span>
      <select class="inp" name="existing_batch_id">
        <option value="">-- Create new session</option>
        {% for batch in batches %}
        <option value="{{ batch.id }}" {% if selected_batch_id == (batch.id|string) %}selected{% endif %}>
          Session #{{ batch.id }}{% if batch.session_name %} -- {{ batch.session_name }}{% endif %} -- {{ batch.contract_date.strftime('%Y-%m-%d') }}
        </option>
        {% endfor %}
      </select>
    </div>
  </div>
  <div class="field">
    <label class="label">Create New Session</label>
    <input class="inp" name="new_session_name" placeholder="Session Name" value="{{ new_session_name_value or '' }}">
  </div>
</div>
      <div class="g g2">
        <div class="field">
          <label class="label">Work Title</label>
          <div class="inp-wrap">
            <span class="inp-ico">&#128395;</span>
            <input class="inp" name="work_title" required placeholder="e.g. La Serenata" value="{{ work_title_value or '' }}">
          </div>
        </div>
        <div class="field">
          <label class="label">Contract Date</label>
          <div class="inp-wrap">
            <span class="inp-ico">&#128197;</span>
            <input class="inp" name="contract_date" type="date" required value="{{ contract_date_value or '' }}">
          </div>
        </div>
      </div>
    </div>
  </div>
  <div class="card">
    <div class="card-hd">
      <div class="card-ico">&#128101;</div>
      <span class="card-title">Writers</span>
      <div class="card-actions">
        <button type="button" class="btn btn-sec btn-sm" onclick="addWriter()">+ Add Writer</button>
      </div>
    </div>
    <div class="card-body" id="writerRows" style="padding-bottom:6px"></div>
  </div>
  <div class="action-bar">
    <button type="button" class="btn btn-sec" onclick="addWriter()">+ Add Writer</button>
    <div class="ab-space"></div>
    <button type="submit" class="btn btn-primary">Save Work to Session</button>
  </div>
  {% if is_modal %}<div id="modal-form-end"></div>{% endif %}
</form>
</div>
</main>
</div>

<div id="writerEditModal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:9999;">
  <div id="writerEditPanel" style="position:absolute;top:5%;left:50%;transform:translateX(-50%);width:92%;max-width:900px;height:88%;background:#0f172a;border:1px solid rgba(255,255,255,.12);border-radius:14px;overflow:hidden;display:flex;flex-direction:column;box-shadow:0 20px 60px rgba(0,0,0,.45);">
    <div style="padding:12px 16px;background:#111827;color:#fff;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid rgba(255,255,255,.08);">
      <div style="display:flex;align-items:center;gap:8px">
        <span style="font-weight:600;">Edit Writer</span>
        <a id="writerModalProfileLink"
           href="#"
           target="_blank"
           class="btn btn-sec btn-sm"
           style="padding:4px 8px">
           View Profile
        </a>
      </div>
      <button type="button" onclick="closeWriterModal()" style="background:none;border:none;color:#fff;font-size:20px;cursor:pointer;">×</button>
    </div>
    <div id="writerModalBody" style="padding:18px;overflow:auto;flex:1;">
      <div style="color:white">Loading writer...</div>
    </div>
  </div>
</div>

""" + _SB_JS + """
<script>
var proMap = {
  BMI: {name:'Songs of Afinarte', ipi:'817874992'},
  ASCAP: {name:'Melodies of Afinarte', ipi:'807953316'},
  SESAC: {name:'Music of Afinarte', ipi:'817094629'}
};
var defAddr = "{{ default_publisher_address }}";
var defCity = "{{ default_publisher_city }}";
var defState = "{{ default_publisher_state }}";
var defZip = "{{ default_publisher_zip }}";
var idx = 0;

function statusHtml(ws, ct) {
  var wc = ws === 'Existing Writer' ? 'tag-exist' : 'tag-new';
  var cc = ct === 'Schedule 1' ? 'tag-s1' : 'tag-full';
  return '<span class="tag ' + wc + '">' + ws + '</span><span class="tag ' + cc + '">' + ct + '</span>';
}

function writerTpl(i, data) {
  data = data || {};
  var h = '<div class="wc open" data-idx="' + i + '">';
  h += '<div class="wc-hd" onclick="toggleWC(this)">';
  h += '<div class="wc-av">&#128100;</div>';
  h += '<div class="wc-nw"><div class="wc-name wc-dn">Writer ' + (i + 1) + '</div>';
  h += '<div class="wc-sub wc-ds">New</div></div>';
  h += '<div class="wc-tags wc-meta">' + statusHtml('New Writer', 'Full Contract') + '</div>';
  h += '<span class="wc-chev">v</span>';
  h += '<button type="button" class="btn btn-danger btn-sm" onclick="rmWriter(event,this)" style="margin-left:8px">Remove</button>';
  h += '</div>';

  h += '<div class="wc-body">';
  h += '<input type="hidden" name="writer_id" class="wid" value="' + (data.selected_writer_id || '') + '">';

  h += '<div class="wc-sec">Identity</div>';
  h += '<div class="g g4" style="gap:10px">';
  h += '<div class="field ac-wrap"><label class="label">First Name</label>';
  h += '<input class="inp wfn" name="writer_first_name" placeholder="First" autocomplete="off" value="' + (data.first_name || '') + '">';
  h += '<div class="ac-box wsug"></div></div>';

  h += '<div class="field"><label class="label">Middle Name</label>';
  h += '<input class="inp wmn" name="writer_middle_name" placeholder="Middle" autocomplete="off" value="' + (data.middle_name || '') + '"></div>';

  h += '<div class="field"><label class="label">Last Name(s)</label>';
  h += '<input class="inp wln" name="writer_last_names" placeholder="Last Name" autocomplete="off" value="' + (data.last_names || '') + '"></div>';

  h += '<div class="field"><label class="label">AKA / Stage</label>';
  h += '<input class="inp waka" name="writer_aka" placeholder="Stage Name" value="' + (data.writer_aka || '') + '"></div>';
  h += '</div>';
   h += '<div class="wc-sec">Writer Address</div>';
  h += '<div class="g g4a" style="gap:10px">';
  h += '<div class="field"><label class="label">Street</label>';
  h += '<input class="inp waddr" name="writer_address" placeholder="Street Address" value="' + (data.address || '') + '"></div>';

  h += '<div class="field"><label class="label">City</label>';
  h += '<input class="inp wcity" name="writer_city" placeholder="City" value="' + (data.city || '') + '"></div>';

  h += '<div class="field"><label class="label">State</label>';
  h += '<input class="inp wst" name="writer_state" placeholder="ST" value="' + (data.state || '') + '"></div>';

  h += '<div class="field"><label class="label">Zip</label>';
  h += '<input class="inp wzip" name="writer_zip_code" placeholder="Zip" value="' + (data.zip_code || '') + '"></div>';
  h += '</div>';

  h += '<div class="wc-sec">Contact</div>';
  h += '<div class="g g2" style="gap:10px">';
  h += '<div class="field"><label class="label">Email</label>';
  h += '<input class="inp wem" name="writer_email" placeholder="writer@email.com" type="email" value="' + (data.email || '') + '"></div>';

  h += '<div class="field"><label class="label">Phone Number</label>';
  h += '<input class="inp wphone" name="writer_phone_number" placeholder="Phone Number" value="' + (data.phone_number || '') + '"></div>';
  h += '</div>';

  h += '<div class="wc-sec">Publishing</div>';
  h += '<div class="g g3" style="gap:10px">';
  h += '<div class="field"><label class="label">Writer %</label>';
  h += '<input class="inp wspl" name="writer_percentage" placeholder="0" type="number" step="0.01" min="0" max="100" value="' + (data.writer_percentage || '') + '"></div>';

  h += '<div class="field"><label class="label">IPI #</label>';
  h += '<input class="inp wipi" name="writer_ipi" placeholder="IPI Number" value="' + (data.ipi || '') + '"></div>';

  h += '<div class="field"><label class="label">PRO</label>';
  h += '<select class="inp wpro" name="writer_pro" onchange="syncPro(this)">';
  h += '<option value="">PRO</option>';
  h += '<option value="BMI"' + ((data.pro || '') === 'BMI' ? ' selected' : '') + '>BMI</option>';
  h += '<option value="ASCAP"' + ((data.pro || '') === 'ASCAP' ? ' selected' : '') + '>ASCAP</option>';
  h += '<option value="SESAC"' + ((data.pro || '') === 'SESAC' ? ' selected' : '') + '>SESAC</option>';
  h += '</select></div>';
  h += '</div>';

  h += '<div class="wc-sec">Publisher Details</div>';
  h += '<div class="g g2" style="gap:10px">';
  h += '<div class="field"><label class="label">Publisher</label>';
  h += '<input class="inp wpub" name="writer_publisher" placeholder="Publisher Name" value="' + (data.publisher || '') + '"></div>';

  h += '<div class="field"><label class="label">Publisher IPI</label>';
  h += '<input class="inp wpipi" name="publisher_ipi" placeholder="Publisher IPI" value="' + (data.publisher_ipi || '') + '" style="max-width:160px"></div>';
  h += '</div>';

  h += '<div class="g g4a" style="gap:10px;margin-top:10px">';
  h += '<div class="field"><label class="label">Address</label>';
  h += '<input class="inp wpaddr" name="publisher_address" value="' + (data.publisher_address || defAddr) + '" placeholder="Address"></div>';

  h += '<div class="field"><label class="label">City</label>';
  h += '<input class="inp wpcity" name="publisher_city" value="' + (data.publisher_city || defCity) + '" placeholder="City"></div>';

  h += '<div class="field"><label class="label">State</label>';
  h += '<input class="inp wpst" name="publisher_state" value="' + (data.publisher_state || defState) + '" placeholder="ST"></div>';

  h += '<div class="field"><label class="label">Zip</label>';
  h += '<input class="inp wpzip" name="publisher_zip_code" value="' + (data.publisher_zip_code || defZip) + '" placeholder="Zip"></div>';
  h += '</div>';


  h += '</div>';
  h += '</div>';
  h += '</div>';

  return h;
}

function toggleWC(hd) { hd.closest('.wc').classList.toggle('open'); }

function reindexWriters() {
  document.querySelectorAll('#writerRows .wc').forEach(function(card, i) {
    card.dataset.idx = i;
    var fn = (card.querySelector('.wfn').value || '').trim();
    var mn = (card.querySelector('.wmn').value || '').trim();
    var ln = (card.querySelector('.wln').value || '').trim();
    if (!fn && !mn && !ln) {
      card.querySelector('.wc-dn').textContent = 'Writer ' + (i + 1);
    }
  });
}

function addWriter() {
  var c = document.getElementById('writerRows');
  c.insertAdjacentHTML('beforeend', writerTpl(idx));
  var card = c.lastElementChild;
  setupWriter(card);
  idx++;
  reindexWriters();
  recalc();
}

function rmWriter(e, btn) {
  e.stopPropagation();
  btn.closest('.wc').remove();
  reindexWriters();
  recalc();
}

function syncPro(sel) {
  var r = sel.closest('.wc');
  var p = proMap[sel.value];
  if (!p) return;
  r.querySelector('.wpub').value = p.name;
  r.querySelector('.wpipi').value = p.ipi;
  updateHdr(r);
}

function fullName(r) {
  return [
    r.querySelector('.wfn').value,
    r.querySelector('.wmn').value,
    r.querySelector('.wln').value
  ].map(function(s) { return s.trim(); }).filter(Boolean).join(' ');
}

function updateHdr(r) {
  var i = parseInt(r.dataset.idx) || 0;
  var n = fullName(r) || 'Writer ' + (i + 1);
  var pro = r.querySelector('.wpro').value || '--';
  var pct = r.querySelector('.wspl').value || '--';
  var writerId = r.querySelector('.wid').value || '';

  if (writerId) {
    r.querySelector('.wc-dn').innerHTML =
      '<button type="button" class="btn btn-sec btn-sm" style="padding:3px 8px" onclick="event.stopPropagation(); openWriterModal(' + writerId + ')">' + n + '</button>';
  } else {
    r.querySelector('.wc-dn').textContent = n;
  }

  r.querySelector('.wc-ds').textContent = pro + ' / ' + pct + '%';
}

function setStatus(r, ws, ct) { r.querySelector('.wc-meta').innerHTML = statusHtml(ws, ct); }
function hideSug(r) { var b = r.querySelector('.wsug'); b.style.display = 'none'; b.innerHTML = ''; }
function resetNew(r) { r.querySelector('.wid').value = ''; setStatus(r, 'New Writer', 'Full Contract'); }

function fillWriter(r, w) {
  r.querySelector('.wid').value = w.id || '';
  var hasNameParts = w.first_name || w.middle_name || w.last_names;
  r.querySelector('.wfn').value = w.first_name || (!hasNameParts ? (w.full_name || '') : '');
  r.querySelector('.wmn').value = w.middle_name || '';
  r.querySelector('.wln').value = w.last_names || '';
  r.querySelector('.waka').value = w.writer_aka || '';
  r.querySelector('.wipi').value = w.ipi || '';
  r.querySelector('.wem').value = w.email || '';
  r.querySelector('.wphone').value = w.phone_number || '';
  r.querySelector('.wpro').value = w.pro || '';
  r.querySelector('.waddr').value = w.address || '';
  r.querySelector('.wcity').value = w.city || '';
  r.querySelector('.wst').value = w.state || '';
  r.querySelector('.wzip').value = w.zip_code || '';
  var pd = proMap[w.pro] || {};
  r.querySelector('.wpub').value = w.default_publisher || pd.name || '';
  r.querySelector('.wpipi').value = w.default_publisher_ipi || pd.ipi || '';
  updateHdr(r);
  setStatus(r, 'Existing Writer', w.has_master_contract ? 'Schedule 1' : 'Full Contract');
  hideSug(r);
}

function setupWriter(r) {
  var fn = r.querySelector('.wfn');
  var mn = r.querySelector('.wmn');
  var ln = r.querySelector('.wln');
  var sug = r.querySelector('.wsug');
  var spl = r.querySelector('.wspl');
  var pro = r.querySelector('.wpro');

  function search() {
    var q = fullName(r);
    if (q.length < 2) { hideSug(r); resetNew(r); return; }
    fetch('/writers/search?q=' + encodeURIComponent(q))
      .then(function(res) { return res.json(); })
      .then(function(ws) {
        if (!ws.length) { hideSug(r); resetNew(r); return; }
        sug.innerHTML = ws.map(function(w) {
          var safeW = JSON.stringify(w).replace(/'/g, "&#39;");
          return "<div class='ac-item' data-w='" + safeW + "'>" +
            "<strong>" + w.full_name + "</strong><br>" +
            "<small>" + (w.city || '') + (w.city && w.state ? ', ' : '') + (w.state || '') + "</small>" +
            "</div>";
        }).join('');
        sug.style.display = 'block';
        sug.querySelectorAll('.ac-item').forEach(function(item) {
          item.addEventListener('click', function() {
            fillWriter(r, JSON.parse(item.dataset.w));
          });
        });
      });
  }

  [fn, mn, ln].forEach(function(inp) {
    inp.addEventListener('input', function() {
      resetNew(r); updateHdr(r); reindexWriters(); search();
    });
  });
  spl.addEventListener('input', function() { updateHdr(r); recalc(); });
  pro.addEventListener('change', function() { updateHdr(r); });
  document.addEventListener('click', function(e) {
    if (![fn, mn, ln, sug].some(function(el) { return el.contains(e.target); })) {
      hideSug(r);
    }
  });
}

function recalc() {
  var total = 0;
  document.querySelectorAll('.wspl').forEach(function(i) {
    total += parseFloat(i.value || 0) || 0;
  });
  var rounded = total.toFixed(2);
  document.getElementById('splitTotal').textContent = rounded;
  var fill = document.getElementById('splitFill');
  var badge = document.getElementById('splitBadge');
  fill.style.width = Math.min(total, 100) + '%';
  if (Math.abs(total - 100) < 0.001) {
    fill.style.background = 'linear-gradient(90deg,#34d399,#059669)';
    badge.className = 'split-badge sb-ok';
    badge.innerHTML = '<span class="sb-dot"></span>Complete';
  } else if (total > 100) {
    fill.style.background = 'linear-gradient(90deg,#ff4f6a,#c0152d)';
    badge.className = 'split-badge sb-over';
    badge.innerHTML = '<span class="sb-dot"></span>Over 100%';
  } else {
    fill.style.background = 'linear-gradient(90deg,var(--a),var(--ae))';
    badge.className = 'split-badge sb-inc';
    badge.innerHTML = '<span class="sb-dot"></span>Incomplete';
  }
}

document.getElementById('workForm').addEventListener('submit', function(e) {
  var rows = document.querySelectorAll('.wc');
  if (!rows.length) { e.preventDefault(); alert('Add at least one writer.'); return; }
  var ok = false;
  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];
    var n = fullName(r);
    var s = parseFloat(r.querySelector('.wspl').value || 0) || 0;
    if (n) { ok = true; if (s <= 0) { e.preventDefault(); alert('Each writer must have a split > 0.'); return; } }
  }
  if (!ok) { e.preventDefault(); alert('Add at least one writer with a name.'); return; }
  var t = parseFloat(document.getElementById('splitTotal').textContent || 0) || 0;
  if (Math.abs(t - 100) >= 0.001) { e.preventDefault(); alert('Total split must equal 100%.'); }
});
var submittedWriters = {{ submitted_writers | tojson | safe }};
if (submittedWriters && submittedWriters.length) {
  submittedWriters.forEach(function(writerData, i) {
    var c = document.getElementById('writerRows');
    c.insertAdjacentHTML('beforeend', writerTpl(idx, writerData));
    var card = c.lastElementChild;
    setupWriter(card);
    updateHdr(card);
    idx++;
  });
  reindexWriters();
  recalc();
} else {
  addWriter();
}

function openWriterModal(writerId) {
  var modal = document.getElementById('writerEditModal');
  var body = document.getElementById('writerModalBody');
  var profileLink = document.getElementById('writerModalProfileLink');
  
  modal.style.display = 'block';
  document.body.style.overflow = 'hidden';
  body.innerHTML = '<div style="color:white">Loading writer...</div>';

  if (profileLink) {
    profileLink.href = '/writers/' + writerId;
  }

  fetch('/writers/' + writerId + '/modal')
    .then(function(res) { return res.text(); })
    .then(function(html) {
      body.innerHTML = html;
    })
    .catch(function() {
      body.innerHTML = '<div style="color:#ff8a8a">Failed to load writer.</div>';
    });
}

function closeWriterModal() {
  document.getElementById('writerEditModal').style.display = 'none';
  document.getElementById('writerModalBody').innerHTML = '<div style="color:white">Loading writer...</div>';
  document.body.style.overflow = 'auto';
}


document.addEventListener('click', function(e) {
  var modal = document.getElementById('writerEditModal');
  if (e.target === modal) {
    closeWriterModal();
  }
});

function refreshWriterCards(writerId) {
  fetch('/writers/search?q=' + encodeURIComponent(''))
    .catch(function(){});

  document.querySelectorAll('#writerRows .wc').forEach(function(card) {
    var selectedId = card.querySelector('.wid') ? card.querySelector('.wid').value : '';
    if (String(selectedId) !== String(writerId)) return;

    fetch('/writers/' + writerId + '/json')
      .then(function(res) { return res.json(); })
      .then(function(w) {
        fillWriter(card, w);
      });
  });
}

function syncModalPro(sel) {
  var pro = sel.value || '';
  var form = sel.closest('form');
  if (!form) return;

  var publisherMap = {
    BMI:   { name: 'Songs of Afinarte',    ipi: '817874992' },
    ASCAP: { name: 'Melodies of Afinarte', ipi: '807953316' },
    SESAC: { name: 'Music of Afinarte',    ipi: '817094629' }
  };

  var p = publisherMap[pro];
  if (!p) return;

  var publisherInp = form.querySelector('input[name="default_publisher"]');
  var publisherIpiInp = form.querySelector('input[name="default_publisher_ipi"]');

  if (publisherInp) publisherInp.value = p.name;
  if (publisherIpiInp) publisherIpiInp.value = p.ipi;
}

</script>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""

# ================================================================
# DUPLICATE WARNING
# ================================================================

DUPLICATE_WARNING_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Possible Duplicate - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("formulario") + """
<main class="main">
""" + _topbar() + """
<div class="page">
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#9888;</div>
    <div>
      <div class="ph-title">Possible Duplicate Found</div>
      <div class="ph-sub">Existing works match this title and writer set.</div>
    </div>
  </div>
</div>
<div class="card">
  <div class="card-hd"><div class="card-ico">&#128269;</div><span class="card-title">Matching Works</span></div>
  <div class="card-body">
    <table class="tbl" style="margin-bottom:18px">
      <thead><tr><th>Title</th><th>Session</th><th>Created</th><th>Action</th></tr></thead>
      <tbody>
        {% for item in duplicates %}
        <tr>
          <td style="font-weight:600">{{ item.title }}</td>
          <td>{{ item.camp_name or '--' }}</td>
          <td style="color:var(--t2)">{{ item.created_at }}</td>
          <td>
            {% if item.batch_id %}
              <button
                type="button"
                class="btn btn-sec btn-sm"
                onclick="openExisting('{{ url_for('publishing.batch_detail', batch_id=item.batch_id) }}')">
                View Existing
              </button>
            {% else %}
              <span style="color:var(--t2)">--</span>
            {% endif %}
          </td>

        </tr>
        {% endfor %}
      </tbody>
    </table>
    <div style="display:flex;gap:10px">
      <form method="post" style="margin:0">
        {% for key, value in form_data.items() %}
          {% if key != "force_create" and key != "return_to_form" %}
            {% if value is string %}
              <input type="hidden" name="{{ key }}" value="{{ value }}">
            {% else %}
              {% for item in value %}
                <input type="hidden" name="{{ key }}" value="{{ item }}">
              {% endfor %}
            {% endif %}
          {% endif %}
        {% endfor %}
        <input type="hidden" name="force_create" value="1">
        <button
          type="submit"
          class="btn btn-danger"
          onclick="return confirm('Are you sure you want to create this duplicate work?')">
          Continue Anyway
        </button>
      </form>

      <form method="post" style="margin:0">
        {% for key, value in form_data.items() %}
          {% if key != "force_create" and key != "return_to_form" %}
            {% if value is string %}
              <input type="hidden" name="{{ key }}" value="{{ value }}">
            {% else %}
              {% for item in value %}
                <input type="hidden" name="{{ key }}" value="{{ item }}">
              {% endfor %}
            {% endif %}
          {% endif %}
        {% endfor %}
        <input type="hidden" name="return_to_form" value="1">
        <button type="submit" class="btn btn-sec">Cancel</button>
      </form>
    </div>
</div>
</div>
</main>
</div>
<div id="existingModal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,0.65);z-index:9999;">
  <div id="existingModalPanel" style="position:absolute;top:5%;left:5%;width:90%;height:90%;background:#0f172a;border:1px solid rgba(255,255,255,0.12);border-radius:14px;overflow:hidden;display:flex;flex-direction:column;box-shadow:0 20px 60px rgba(0,0,0,0.45);transform:translateY(12px);opacity:0;transition:all .18s ease;">
    
    <div style="padding:12px 16px;background:#111827;color:#fff;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid rgba(255,255,255,0.08);flex-shrink:0;">
      <span style="font-weight:600;">Existing Session</span>
      <button type="button" onclick="closeExisting()" style="background:none;border:none;color:#fff;font-size:20px;cursor:pointer;">×</button>
    </div>

    <div id="modalLoader" style="flex:1;display:flex;align-items:center;justify-content:center;color:white;font-size:14px;">
      Loading session...
    </div>

    <iframe id="existingFrame" style="flex:1;border:none;background:#fff;display:none;"></iframe>
  </div>
</div>
""" + _SB_JS + """
<script>
function openExisting(url) {
  var modal = document.getElementById('existingModal');
  var panel = document.getElementById('existingModalPanel');
  var frame = document.getElementById('existingFrame');
  var loader = document.getElementById('modalLoader');

  loader.style.display = 'flex';
  frame.style.display = 'none';
  frame.src = '';
  modal.style.display = 'block';
  document.body.style.overflow = 'hidden';

  requestAnimationFrame(function() {
    panel.style.opacity = '1';
    panel.style.transform = 'translateY(0)';
  });

  frame.onload = function() {
    try {
      var doc = frame.contentDocument || frame.contentWindow.document;

      // Hide navigation / controls for cleaner preview
      var selectorsToHide = [
        '.sb',
        '.topbar',
        '.ph-actions',
        '.action-bar',
        '#genForm',
        '.ds-form',
        '.upl-form',
        'a.btn',
        'button'
      ];

      selectorsToHide.forEach(function(sel) {
        doc.querySelectorAll(sel).forEach(function(el) {
          el.style.display = 'none';
        });
      });

      // Remove left margin caused by hidden sidebar
      doc.querySelectorAll('.main').forEach(function(el) {
        el.style.marginLeft = '0';
      });

      // Tighten page padding a bit
      doc.querySelectorAll('.page').forEach(function(el) {
        el.style.padding = '16px';
      });

      // Disable all links so nothing navigates inside popup
      doc.querySelectorAll('a').forEach(function(a) {
        a.removeAttribute('href');
        a.style.pointerEvents = 'none';
        a.style.cursor = 'default';
        a.style.textDecoration = 'none';
      });

      // Disable forms and inputs
      doc.querySelectorAll('form').forEach(function(f) {
        f.addEventListener('submit', function(e) {
          e.preventDefault();
          return false;
        });
      });

      doc.querySelectorAll('input, select, textarea, button').forEach(function(el) {
        el.disabled = true;
        el.style.pointerEvents = 'none';
      });
    } catch (e) {
      console.log('Preview cleanup skipped:', e);
    }

    loader.style.display = 'none';
    frame.style.display = 'block';
  };

  frame.src = url;
}

function closeExisting() {
  var modal = document.getElementById('existingModal');
  var panel = document.getElementById('existingModalPanel');
  var frame = document.getElementById('existingFrame');
  var loader = document.getElementById('modalLoader');

  panel.style.opacity = '0';
  panel.style.transform = 'translateY(12px)';
  document.body.style.overflow = 'auto';

  setTimeout(function() {
    modal.style.display = 'none';
    frame.src = '';
    frame.style.display = 'none';
    loader.style.display = 'flex';
  }, 180);
}

document.addEventListener('click', function(e) {
  var modal = document.getElementById('existingModal');
  if (e.target === modal) {
    closeExisting();
  }
});
</script>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""

# ================================================================
# WORKS LIST
# ================================================================

WORKS_LIST_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Works - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("works_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128395;</div>
    <div><div class="ph-title">Works</div><div class="ph-sub">All registered musical works</div></div>
  </div>
  <div class="ph-actions"><a href="/" class="btn btn-primary">+ New Work</a></div>
</div>
<div class="card">
  <div class="card-hd"><div class="card-ico">&#128269;</div><span class="card-title">Search</span></div>
  <div class="card-body">
    <form method="get" style="display:flex;gap:8px;flex-wrap:wrap">
      <input class="inp" name="q" value="{{ q }}" placeholder="Search by work title, writer, or IPI..." style="max-width:340px">

      <select class="inp" name="sort" style="max-width:220px">
        <option value="newest" {% if sort == "newest" %}selected{% endif %}>Newest First</option>
        <option value="oldest" {% if sort == "oldest" %}selected{% endif %}>Oldest First</option>
        <option value="title_asc" {% if sort == "title_asc" %}selected{% endif %}>Title A-Z</option>
        <option value="title_desc" {% if sort == "title_desc" %}selected{% endif %}>Title Z-A</option>
      </select>

      <button class="btn btn-sec" type="submit">Apply</button>

      {% if q or sort != "newest" %}
        <a href="/works" class="btn btn-sec">Clear</a>
      {% endif %}
    </form>
  </div>
</div>
<div class="card">
  <div class="card-hd"><div class="card-ico">&#128203;</div><span class="card-title">All Works</span></div>
  <div class="tbl-wrap">
    <table class="tbl tbl-works" style="table-layout:auto">
      <thead>
        <tr>
          <th style="width:30%">Work Title</th>
          <th>Writers</th>
          <th style="white-space:nowrap">Date</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody>
        {% for work in works %}
        {% set docs = work.contract_documents %}
        {% set ns = namespace(ds_st=none,any_signed=false) %}
        {% for d in docs %}
          {% if d.docusign_status and not ns.ds_st %}{% set ns.ds_st = d.docusign_status %}{% endif %}
          {% if d.status in ['signed','signed_uploaded','signed_complete'] %}{% set ns.any_signed = true %}{% endif %}
        {% endfor %}
        {% set signed_docs = docs | selectattr('signed_pdf_drive_web_view_link') | list %}
        <tr class="work-row" data-work="{{ work.id }}" onclick="toggleWork({{ work.id }})">
          <td>
            <span class="expand-chevron">&#9658;</span>
            <span style="font-weight:600">{{ work.title }}</span>
            {% if work.contract_date %}<div style="font-size:11px;color:var(--t3);margin-top:2px;margin-left:16px">{{ work.contract_date.strftime('%b %d, %Y') }}</div>{% endif %}
          </td>
          <td>
            <div class="writer-preview">
              {% for ww in work.work_writers[:2] %}
              <span class="writer-pill">
                <span class="wp-name">{{ ww.writer.full_name }}</span>
                {% if ww.writer.ipi %}<span class="wp-meta">{{ ww.writer.ipi }}</span>{% endif %}
                {% if ww.writer.pro %}<span class="wp-meta">{{ ww.writer.pro }}</span>{% endif %}
                <span class="wp-split">{{ "%.0f"|format(ww.writer_percentage) }}%</span>
              </span>
              {% endfor %}
              {% if work.work_writers|length > 2 %}
              <span style="font-size:11px;color:var(--t3)">+{{ work.work_writers|length - 2 }} more</span>
              {% endif %}
            </div>
          </td>
          <td style="white-space:nowrap;font-size:12px;color:var(--t2)">{{ work.contract_date.strftime('%b %d, %Y') if work.contract_date else '--' }}</td>
          <td>
            {% if ns.any_signed %}<span class="tag tag-s1">Signed</span>
            {% elif ns.ds_st %}<span class="status s-{{ ns.ds_st }}"><span class="status-dot"></span>{{ ns.ds_st | title }}</span>
            {% elif docs %}<span style="color:var(--t3);font-size:11px">Pending</span>
            {% else %}<span style="color:var(--t3);font-size:11px">--</span>{% endif %}
          </td>
        </tr>
        <tr class="work-detail-row" id="detail-{{ work.id }}">
          <td colspan="4">
            <div class="work-detail-inner">
              <div class="wd-section">
                <div class="wd-label">Writers &amp; Splits</div>
                <table class="wd-writers-tbl">
                  <thead><tr><th>Writer</th><th>IPI</th><th>PRO</th><th>Split</th></tr></thead>
                  <tbody>
                    {% for ww in work.work_writers %}
                    <tr>
                      <td style="font-weight:600">{{ ww.writer.full_name }}</td>
                      <td style="font-family:var(--fm)">{{ ww.writer.ipi or '--' }}</td>
                      <td>{{ ww.writer.pro or '--' }}</td>
                      <td style="color:var(--a);font-weight:700">{{ "%.2f"|format(ww.writer_percentage) }}%</td>
                    </tr>
                    {% endfor %}
                  </tbody>
                </table>
              </div>
              <div class="wd-section">
                <div class="wd-label">Session</div>
                <div style="font-size:13px;color:var(--t2)">
                  {% if work.batch %}
                    <a href="/batches/{{ work.batch_id }}" style="color:var(--a)">{{ work.batch.session_name or 'Session #' ~ work.batch_id }}</a>
                  {% else %}--{% endif %}
                </div>
                <div class="wd-label" style="margin-top:12px">Documents</div>
                <div style="display:flex;flex-wrap:wrap;gap:6px">
                  {% if docs|length == 1 and docs[0].drive_web_view_link %}
                    <a href="{{ docs[0].drive_web_view_link }}" target="_blank" class="btn btn-sec btn-xs">&#128196; Contract</a>
                  {% elif docs|length > 1 %}
                    {% for d in docs %}{% if d.drive_web_view_link %}<a href="{{ d.drive_web_view_link }}" target="_blank" class="btn btn-sec btn-xs">&#128196; {{ d.writer_name_snapshot.split()[0] }}</a>{% endif %}{% endfor %}
                  {% else %}<span style="font-size:12px;color:var(--t3)">None generated</span>{% endif %}
                  {% for d in signed_docs %}
                    <a href="{{ d.signed_pdf_drive_web_view_link }}" target="_blank" class="btn btn-success btn-xs">&#128209; Signed</a>
                  {% endfor %}
                </div>
                <div style="display:flex;gap:8px;margin-top:16px">
                  <a href="/works/{{ work.id }}/edit" class="btn btn-primary btn-sm" style="color:#fff" onclick="event.stopPropagation()">Edit</a>
                  <a href="/works/{{ work.id }}" class="btn btn-sec btn-sm" onclick="event.stopPropagation()">Full View</a>
                </div>
              </div>
            </div>
          </td>
        </tr>
        {% endfor %}
        {% if not works %}<tr class="empty"><td colspan="4">No works found{% if q %} for "{{ q }}"{% endif %}.</td></tr>{% endif %}
      </tbody>
    </table>
  </div>
  {% if pagination.pages > 1 %}
  <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-top:1px solid var(--b1);font-size:13px;color:var(--t2)">
    <span>{{ pagination.total }} works &mdash; page {{ pagination.page }} of {{ pagination.pages }}</span>
    <div style="display:flex;gap:6px">
      {% if pagination.has_prev %}
        <a href="?q={{ q }}&sort={{ sort }}&page={{ pagination.prev_num }}" class="btn btn-sec btn-sm">&#8592; Prev</a>
      {% endif %}
      {% if pagination.has_next %}
        <a href="?q={{ q }}&sort={{ sort }}&page={{ pagination.next_num }}" class="btn btn-sec btn-sm">Next &#8594;</a>
      {% endif %}
    </div>
  </div>
  {% endif %}
</div>
</div>
</main>
</div>
""" + _SB_JS + """
<script>
function toggleWork(id) {
  var row = document.getElementById('detail-' + id);
  var header = document.querySelector('[data-work="' + id + '"]');
  var isOpen = row.classList.contains('open');
  document.querySelectorAll('.work-detail-row.open').forEach(function(r){ r.classList.remove('open'); });
  document.querySelectorAll('.work-row.open').forEach(function(r){ r.classList.remove('open'); });
  if (!isOpen) {
    row.classList.add('open');
    header.classList.add('open');
    row.scrollIntoView({behavior:'smooth', block:'nearest'});
  }
}
</script>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""

# ================================================================
# SESSIONS LIST
# ================================================================

BATCHES_LIST_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sessions - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("batches_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128466;</div>
    <div><div class="ph-title">Sessions</div><div class="ph-sub">Groups of works ready for contract generation</div></div>
  </div>
  <div class="ph-actions" style="flex-shrink:0"><a href="/" class="btn btn-primary">+ New Work</a></div>
</div>
<div class="card">
  <div class="card-hd"><div class="card-ico">&#128269;</div><span class="card-title">Search</span></div>
  <div class="card-body">
    <form method="get" style="display:flex;gap:8px;flex-wrap:wrap">
      <input class="inp" name="q" value="{{ q }}" placeholder="Search by session name, work title, or writer..." style="max-width:340px">
      <select class="inp" name="sort" style="max-width:220px">
        <option value="newest" {% if sort == "newest" %}selected{% endif %}>Newest First</option>
        <option value="oldest" {% if sort == "oldest" %}selected{% endif %}>Oldest First</option>
        <option value="title_asc" {% if sort == "title_asc" %}selected{% endif %}>Name A-Z</option>
        <option value="title_desc" {% if sort == "title_desc" %}selected{% endif %}>Name Z-A</option>
      </select>
      <button class="btn btn-sec" type="submit">Apply</button>
      {% if q or sort != "newest" %}<a href="/batches" class="btn btn-sec">Clear</a>{% endif %}
    </form>
  </div>
</div>
<div class="card">
  <div class="card-hd"><div class="card-ico">&#128466;</div><span class="card-title">All Sessions</span></div>
  <div class="tbl-wrap">
    <table class="tbl" style="table-layout:auto">
      <thead><tr><th>Session</th><th>Date</th><th>Status</th><th>Works</th></tr></thead>
      <tbody>
        {% for batch in batches %}
        <tr class="sess-row" data-batch="{{ batch.id }}" onclick="toggleSession({{ batch.id }})">
          <td>
            <span class="expand-chevron">&#9658;</span>
            <span style="font-weight:600">{{ batch.session_name or 'Session #' ~ batch.id }}</span>
            <div style="font-size:11px;color:var(--t3);margin-top:2px;margin-left:16px">Session #{{ batch.id }}</div>
          </td>
          <td style="font-size:12px;color:var(--t2);white-space:nowrap">{{ batch.contract_date.strftime('%b %d, %Y') }}</td>
          <td><span class="status s-{{ batch.status }}"><span class="status-dot"></span>{{ batch.status | replace('_',' ') | title }}</span></td>
          <td><span style="background:rgba(99,133,255,.1);color:var(--a);border:1px solid rgba(99,133,255,.2);border-radius:99px;padding:2px 8px;font-size:11px;font-weight:700">{{ work_counts.get(batch.id, 0) }}</span></td>
        </tr>
        <tr class="sess-detail-row" id="sdetail-{{ batch.id }}">
          <td colspan="4">
            <div class="sess-detail-inner">
              <div style="margin-bottom:12px">
                <div class="wd-label">Works in this session</div>
                {% set batch_works = session_works.get(batch.id, []) %}
                {% if batch_works %}
                <table class="sd-works-tbl">
                  <thead><tr><th>Work Title</th><th>Writers</th><th>Contract Date</th></tr></thead>
                  <tbody>
                    {% for w in batch_works %}
                    <tr>
                      <td style="font-weight:600">{{ w.title }}</td>
                      <td style="color:var(--t2)">{{ w.work_writers|length }} writer{{ 's' if w.work_writers|length != 1 else '' }}</td>
                      <td style="color:var(--t3)">{{ w.contract_date.strftime('%b %d, %Y') if w.contract_date else '--' }}</td>
                    </tr>
                    {% endfor %}
                  </tbody>
                </table>
                {% else %}
                <span style="font-size:12px;color:var(--t3)">No works yet.</span>
                {% endif %}
              </div>
              <div style="display:flex;gap:8px;margin-top:4px;align-items:flex-start;width:fit-content">
                <a href="/batches/{{ batch.id }}" class="btn btn-primary btn-xs" style="color:#fff" onclick="event.stopPropagation()">View Session</a>
                <a href="/?batch_id={{ batch.id }}" class="btn btn-sec btn-xs" onclick="event.stopPropagation()">+ Add Work</a>
              </div>
            </div>
          </td>
        </tr>
        {% endfor %}
        {% if not batches %}<tr class="empty"><td colspan="4">No sessions found{% if q %} for "{{ q }}"{% endif %}.</td></tr>{% endif %}
      </tbody>
    </table>
  </div>
  {% if pagination.pages > 1 %}
  <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-top:1px solid var(--b1);font-size:13px;color:var(--t2)">
    <span>{{ pagination.total }} sessions &mdash; page {{ pagination.page }} of {{ pagination.pages }}</span>
    <div style="display:flex;gap:6px">
      {% if pagination.has_prev %}<a href="?q={{ q }}&sort={{ sort }}&page={{ pagination.prev_num }}" class="btn btn-sec btn-sm">&#8592; Prev</a>{% endif %}
      {% if pagination.has_next %}<a href="?q={{ q }}&sort={{ sort }}&page={{ pagination.next_num }}" class="btn btn-sec btn-sm">Next &#8594;</a>{% endif %}
    </div>
  </div>
  {% endif %}
</div>
</div>
</main>
</div>
""" + _SB_JS + """
<script>
function toggleSession(id) {
  var row = document.getElementById('sdetail-' + id);
  var header = document.querySelector('[data-batch="' + id + '"]');
  var isOpen = row.classList.contains('open');
  document.querySelectorAll('.sess-detail-row.open').forEach(function(r){ r.classList.remove('open'); });
  document.querySelectorAll('.sess-row.open').forEach(function(r){ r.classList.remove('open'); });
  if (!isOpen) {
    row.classList.add('open');
    header.classList.add('open');
    row.scrollIntoView({behavior:'smooth', block:'nearest'});
  }
}
</script>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""

# ================================================================
# SESSION DETAIL
# ================================================================

BATCH_DETAIL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Session {{ batch.id }} - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("batches_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
{% with messages = get_flashed_messages() %}{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128466;</div>
    <div>
      <div class="ph-title">Session #{{ batch.id }}</div>
      <div class="ph-sub">{{ batch.session_name or 'No name' }} - {{ batch.contract_date.strftime('%b %d, %Y') }}</div>
    </div>
  </div>
  <div class="ph-actions">
    <a href="/batches" class="btn btn-sec btn-sm">Back</a>
  </div>
</div>
<div class="card">
  <div class="card-hd"><div class="card-ico">&#8505;</div><span class="card-title">Session Info</span></div>
  <div class="card-body">
    <div class="info-grid">
      <div class="info-item"><label>Session Name</label><span>{{ batch.session_name or '--' }}</span></div>
      <div class="info-item"><label>Contract Date</label><span>{{ batch.contract_date.strftime('%B %d, %Y') }}</span></div>
      <div class="info-item"><label>Status</label><span class="status s-{{ batch.status }}"><span class="status-dot"></span>{{ batch.status | replace('_',' ') | title }}</span></div>
      <div class="info-item"><label>Created</label><span>{{ batch.created_at.strftime('%b %d, %Y %H:%M') }}</span></div>
    </div>
  </div>
</div>
<div class="card">
  <div class="card-hd"><div class="card-ico">&#128395;</div><span class="card-title">Works in Session</span></div>
  <div class="tbl-wrap">
    <table class="tbl tbl-batch-works" style="table-layout:auto">
      <thead><tr><th>Work Title</th><th>Writers</th><th>Date</th></tr></thead>
      <tbody>
        {% for work in works %}
        <tr class="work-row" data-work="{{ work.id }}" onclick="toggleWork({{ work.id }})">
          <td>
            <span class="expand-chevron">&#9658;</span>
            <span style="font-weight:600">{{ work.title }}</span>
          </td>
          <td>
            <div class="writer-preview">
              {% for ww in work.work_writers[:2] %}
              <span class="writer-pill">
                <span class="wp-name">{{ ww.writer.full_name }}</span>
                <span class="wp-split">{{ "%.0f"|format(ww.writer_percentage) }}%</span>
              </span>
              {% endfor %}
              {% if work.work_writers|length > 2 %}
              <span style="font-size:11px;color:var(--t3)">+{{ work.work_writers|length - 2 }} more</span>
              {% endif %}
            </div>
          </td>
          <td style="font-size:12px;color:var(--t3);white-space:nowrap">{{ work.created_at.strftime('%b %d, %Y') }}</td>
        </tr>
        <tr class="work-detail-row" id="detail-{{ work.id }}">
          <td colspan="3">
            <div class="work-detail-inner">
              <div class="wd-section">
                <div class="wd-label">Writers &amp; Splits</div>
                <table class="wd-writers-tbl">
                  <thead><tr><th>Writer</th><th>IPI</th><th>PRO</th><th>Split</th></tr></thead>
                  <tbody>
                    {% for ww in work.work_writers %}
                    <tr>
                      <td style="font-weight:600">{{ ww.writer.full_name }}</td>
                      <td style="font-family:var(--fm)">{{ ww.writer.ipi or '--' }}</td>
                      <td>{{ ww.writer.pro or '--' }}</td>
                      <td style="color:var(--a);font-weight:700">{{ "%.2f"|format(ww.writer_percentage) }}%</td>
                    </tr>
                    {% endfor %}
                  </tbody>
                </table>
              </div>
              <div class="wd-section">
                <div class="wd-label">Contract Date</div>
                <div style="font-size:13px;color:var(--t2);margin-bottom:14px">{{ work.contract_date.strftime('%B %d, %Y') if work.contract_date else '--' }}</div>
                <div style="display:flex;gap:8px">
                  <a href="/works/{{ work.id }}/edit" class="btn btn-primary btn-sm" style="color:#fff" onclick="event.stopPropagation()">Edit Work</a>
                  <a href="/works/{{ work.id }}" class="btn btn-sec btn-sm" onclick="event.stopPropagation()">Full View</a>
                </div>
              </div>
            </div>
          </td>
        </tr>
        {% endfor %}
        {% if not works %}<tr class="empty"><td colspan="3">No works in this session.</td></tr>{% endif %}
      </tbody>
    </table>
  </div>
</div>
<div class="card">
  <div class="card-hd"><div class="card-ico">&#128101;</div><span class="card-title">Writer Summary</span></div>
  <div class="tbl-wrap">
    <table class="tbl tbl-batch-writers">
      <thead><tr><th>Writer</th><th>AKA</th><th>IPI</th><th>PRO</th><th>Works</th><th>Publishing Contract</th></tr></thead>
      <tbody>
        {% for item in writer_summary %}
        <tr>
          <td style="font-weight:600">{{ item.writer.full_name }}</td>
          <td style="color:var(--t2)">{{ item.writer.writer_aka or '--' }}</td>
          <td style="font-family:var(--fm);font-size:12px;color:var(--t2)">{{ item.writer.ipi or '--' }}</td>
          <td><span class="tag tag-full">{{ item.writer.pro or '--' }}</span></td>
          <td>{{ item.work_count }}</td>
          <td>{% if item.writer.has_master_contract %}<span class="tag tag-s1">Yes</span>{% else %}<span style="color:var(--t3)">No</span>{% endif %}</td>
        </tr>
        {% endfor %}
        {% if not writer_summary %}<tr class="empty"><td colspan="6">No writers in this session.</td></tr>{% endif %}
      </tbody>
    </table>
  </div>
</div>
<div class="card">
  <div class="card-hd"><div class="card-ico">&#128196;</div><span class="card-title">Generated Documents</span></div>
  <div class="tbl-wrap">
    <table class="tbl tbl-docs">
      <thead>
        <tr>
          <th>Writer</th><th>Type</th><th>File</th><th>Generated</th>
          <th>DocuSign</th><th>DS Status</th><th>Certificate</th>
          <th>Upload Signed</th><th>Signed PDF</th><th>Status</th>
        </tr>
      </thead>
      <tbody id="generatedDocumentsBody">
        {% for doc in documents %}
        <tr data-doc-id="{{ doc.id }}">
          <td data-label="Writer" style="font-weight:600">{{ doc.writer_name_snapshot }}</td>
          <td data-label="Type"><span class="tag tag-full">{{ doc.document_type }}</span></td>
          <td data-label="File" data-hide-mobile>
            {% if doc.drive_web_view_link %}
              <a href="{{ doc.drive_web_view_link }}" target="_blank" class="file-link" title="{{ doc.file_name }}">&#128196; {{ doc.file_name | truncate(30,true,'...') }}</a>
            {% else %}
              <span class="file-link-plain">{{ doc.file_name }}</span>
            {% endif %}
          </td>
          <td data-label="Generated" data-hide-mobile style="color:var(--t3);font-size:11.5px">{{ doc.generated_at.strftime('%b %d %H:%M') if doc.generated_at else '--' }}</td>
          <td data-label="DocuSign">
            <form method="post" action="/documents/{{ doc.id }}/send-docusign" class="ds-form">
              <button type="submit" class="btn btn-sec btn-xs ds-btn">
                <span class="ds-lbl">{% if doc.docusign_status == 'completed' %}Resend{% elif doc.docusign_status == 'sent' %}Sent{% elif doc.docusign_status == 'delivered' %}Delivered{% else %}Send{% endif %}</span>
                <span class="spin ds-spin"></span>
              </button>
            </form>
          </td>
          <td data-label="DS Status">{% if doc.docusign_status %}<span class="status s-{{ doc.docusign_status }}"><span class="status-dot"></span>{{ doc.docusign_status | title }}</span>{% else %}--{% endif %}</td>
          <td data-label="Certificate" data-hide-mobile>{% if doc.certificate_drive_web_view_link %}<a href="{{ doc.certificate_drive_web_view_link }}" target="_blank" class="btn btn-sec btn-xs">Cert</a>{% else %}--{% endif %}</td>
          <td data-label="Upload Signed">
            <form method="post" action="/documents/{{ doc.id }}/upload-signed" enctype="multipart/form-data" class="upl-form">
              <input type="file" name="signed_file" class="upl-inp" required>
              <button type="submit" class="btn btn-success btn-xs">Upload</button>
            </form>
          </td>
          <td data-label="Signed PDF">
            {% if doc.signed_pdf_drive_web_view_link %}<a href="{{ doc.signed_pdf_drive_web_view_link }}" target="_blank" class="file-link">&#128209; Signed</a>
            {% elif doc.signed_web_view_link %}<a href="{{ doc.signed_web_view_link }}" target="_blank" class="file-link">&#128209; Signed</a>
            {% else %}--{% endif %}
          </td>
          <td data-label="Status">{% if doc.status %}<span class="status s-{{ doc.status }}"><span class="status-dot"></span>{{ doc.status | replace('_',' ') | title }}</span>{% else %}--{% endif %}</td>
        </tr>
        {% endfor %}
        {% if not documents %}<tr class="empty"><td colspan="10">No documents generated yet. Click Generate Docs above.</td></tr>{% endif %}
      </tbody>
    </table>
  </div>
</div>
</div>
</main>
</div>
""" + _SB_JS + """
<script>
function toggleWork(id) {
  var row = document.getElementById('detail-' + id);
  var header = document.querySelector('[data-work="' + id + '"]');
  var isOpen = row.classList.contains('open');
  document.querySelectorAll('.work-detail-row.open').forEach(function(r){ r.classList.remove('open'); });
  document.querySelectorAll('.work-row.open').forEach(function(r){ r.classList.remove('open'); });
  if (!isOpen) {
    row.classList.add('open');
    header.classList.add('open');
    row.scrollIntoView({behavior:'smooth', block:'nearest'});
  }
}
var batchId = {{ batch.id }};

function esc(v) {
  return (v || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function renderFileCell(doc) {
  if (doc.drive_web_view_link) {
    var short = doc.file_name.length > 30 ? doc.file_name.substring(0, 30) + '...' : doc.file_name;
    return '<a href="' + doc.drive_web_view_link + '" target="_blank" class="file-link" title="' + esc(doc.file_name) + '">&#128196; ' + esc(short) + '</a>';
  }
  return '<span class="file-link-plain">' + esc(doc.file_name) + '</span>';
}

function renderDsBtn(doc) {
  var url = '/documents/' + doc.id + '/send-docusign';
  var lbl = 'Send';
  if (doc.docusign_status === 'completed') lbl = 'Resend';
  else if (doc.docusign_status === 'delivered') lbl = 'Delivered';
  else if (doc.docusign_status === 'sent') lbl = 'Sent';
  return '<form method="post" action="' + url + '" class="ds-form"><button type="submit" class="btn btn-sec btn-xs ds-btn"><span class="ds-lbl">' + lbl + '</span><span class="spin ds-spin"></span></button></form>';
}

function renderStatus(val, cls) {
  if (!val) return '--';
  var c = cls + val.replace(/[ _]/g, '_');
  var l = val.replace(/_/g, ' ').replace(/(^| )[a-z]/g, function(x) { return x.toUpperCase(); });
  return '<span class="status ' + c + '"><span class="status-dot"></span>' + l + '</span>';
}

function updateDocs(data) {
  var tb = document.getElementById('generatedDocumentsBody');
  if (!tb || !data.documents) return;
  tb.innerHTML = data.documents.map(function(doc) {
    var signedCell = '--';
    if (doc.signed_pdf_drive_web_view_link) {
      signedCell = '<a href="' + doc.signed_pdf_drive_web_view_link + '" target="_blank" class="file-link">&#128209; Signed</a>';
    } else if (doc.signed_web_view_link) {
      signedCell = '<a href="' + doc.signed_web_view_link + '" target="_blank" class="file-link">&#128209; Signed</a>';
    }
    var certCell = doc.certificate_drive_web_view_link
      ? '<a href="' + doc.certificate_drive_web_view_link + '" target="_blank" class="btn btn-sec btn-xs">Cert</a>'
      : '--';
    return '<tr data-doc-id="' + doc.id + '">'
      + '<td data-label="Writer" style="font-weight:600">' + esc(doc.writer_name_snapshot) + '</td>'
      + '<td data-label="Type"><span class="tag tag-full">' + esc(doc.document_type) + '</span></td>'
      + '<td data-label="File" data-hide-mobile>' + renderFileCell(doc) + '</td>'
      + '<td data-label="Generated" data-hide-mobile style="color:var(--t3);font-size:11.5px">' + (doc.generated_at || '--') + '</td>'
      + '<td data-label="DocuSign">' + renderDsBtn(doc) + '</td>'
      + '<td data-label="DS Status">' + renderStatus(doc.docusign_status, 's-') + '</td>'
      + '<td data-label="Certificate" data-hide-mobile>' + certCell + '</td>'
      + '<td data-label="Upload Signed"><form method="post" action="/documents/' + doc.id + '/upload-signed" enctype="multipart/form-data" class="upl-form"><input type="file" name="signed_file" class="upl-inp" required><button type="submit" class="btn btn-success btn-xs">Upload</button></form></td>'
      + '<td data-label="Signed PDF">' + signedCell + '</td>'
      + '<td data-label="Status">' + renderStatus(doc.status, 's-') + '</td>'
      + '</tr>';
  }).join('');
  bindDs();
  if (data.documents.length > 0) stopGenSpin();
}

var _pollTimer = null;
var _terminalStatuses = ['completed','declined','voided'];

function poll() {
  fetch('/batches/' + batchId + '/status-json', {cache: 'no-store'})
    .then(function(r) { if (r.ok) return r.json(); })
    .then(function(d) {
      if (!d) return;
      updateDocs(d);
      // stop polling once every doc with a docusign envelope is in a terminal state
      if (d.documents && d.documents.length > 0) {
        var allDone = d.documents.every(function(doc) {
          if (!doc.docusign_envelope_id) return true; // no envelope, not waiting
          return _terminalStatuses.indexOf(doc.docusign_status) !== -1;
        });
        if (allDone && _pollTimer) {
          clearInterval(_pollTimer);
          _pollTimer = null;
        }
      }
    })
    .catch(function(e) { console.error(e); });
}

function stopGenSpin() {
  var btn = document.getElementById('genBtn');
  if (!btn) return;
  btn.disabled = false;
  document.getElementById('genSpin').classList.remove('on');
  document.getElementById('genLabel').textContent = 'Generate Docs';
}

function bindDs() {
  document.querySelectorAll('.ds-form').forEach(function(f) {
    if (f.dataset.bound) return;
    f.dataset.bound = '1';
    f.addEventListener('submit', function(e) {
      e.preventDefault();
      var btn = f.querySelector('.ds-btn');
      var spin = f.querySelector('.ds-spin');
      var lbl = f.querySelector('.ds-lbl');
      if (btn) btn.disabled = true;
      if (spin) spin.classList.add('on');
      if (lbl) lbl.textContent = 'Sending...';
      setTimeout(function() { f.submit(); }, 150);
    });
  });
}

document.addEventListener('DOMContentLoaded', function() {
  bindDs();
  var gf = document.getElementById('genFormMobile');
  if (gf) {
    gf.addEventListener('submit', function(e) {
      e.preventDefault();
      var btn = gf.querySelector('button');
      if (btn) { btn.disabled = true; btn.textContent = 'Generating...'; }
      setTimeout(function() { gf.submit(); }, 150);
      setTimeout(function() { window.location.reload(); }, 5000);
    });
  }
  _pollTimer = setInterval(poll, 5000);
});
</script>
<div class="action-bar">
  <a href="/?batch_id={{ batch.id }}" class="btn btn-sec">+ Add Work</a>
  <div class="ab-space"></div>
  <form method="post" action="/batches/{{ batch.id }}/generate" id="genFormMobile" style="flex:1 1 auto;display:flex">
    <button type="submit" class="btn btn-primary" style="flex:1 1 auto;justify-content:center">
      <span>Generate Docs</span>
    </button>
  </form>
</div>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""

# ================================================================
# WORK DETAIL
# ================================================================

WORK_DETAIL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ work.title }} - LabelMind</title>""" + _STYLE + """
<style>@media(max-width:767px){.ph-actions{display:none}}</style>
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("works_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128395;</div>
    <div>
      <div class="ph-title">{{ work.title }}</div>
      <div class="ph-sub">{{ work.batch.session_name if work.batch and work.batch.session_name else 'No session' }} - {{ work.contract_date.strftime('%b %d, %Y') if work.contract_date else '--' }}</div>
    </div>
  </div>
  <div class="ph-actions">
    <a href="/works/{{ work.id }}/edit" class="btn btn-primary btn-sm">Edit Work</a>
    {% if work.batch_id %}<a href="/batches/{{ work.batch_id }}" class="btn btn-sec btn-sm">View Session</a>{% endif %}
    <form method="post" action="/works/{{ work.id }}/delete" style="display:inline" onsubmit="return confirm('Delete this work? This will remove its writer links and generated documents.');">
      <button type="submit" class="btn btn-danger btn-sm">Delete Work</button>
    </form>
    <a href="/works" class="btn btn-sec btn-sm">Back</a>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#128203;</div><span class="card-title">Work Info</span></div>
  <div class="card-body">
    <div class="info-grid">
      <div class="info-item"><label>Session Name</label><span>{{ work.batch.session_name if work.batch and work.batch.session_name else '--' }}</span></div>
      <div class="info-item"><label>Session</label>{% if work.batch_id %}<a href="/batches/{{ work.batch_id }}">Session #{{ work.batch_id }}</a>{% else %}<span>--</span>{% endif %}</div>
      <div class="info-item"><label>Contract Date</label><span>{{ work.contract_date.strftime('%B %d, %Y') if work.contract_date else '--' }}</span></div>
      <div class="info-item"><label>Created</label><span>{{ work.created_at.strftime('%b %d, %Y %H:%M') }}</span></div>
    </div>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#128101;</div><span class="card-title">Writers &amp; Splits</span></div>
  <div class="tbl-wrap">
    <table class="tbl tbl-work-writers">
      <thead><tr><th>Writer</th><th>AKA</th><th>IPI</th><th>PRO</th><th>Split %</th><th>Publisher</th><th>Pub IPI</th><th>Publishing</th></tr></thead>
      <tbody>
        {% for ww in work.work_writers %}
        <tr>
          <td style="font-weight:600">{{ ww.writer.full_name }}</td>
          <td style="color:var(--t2)">{{ ww.writer.writer_aka or '--' }}</td>
          <td style="font-family:var(--fm);font-size:12px;color:var(--t2)">{{ ww.writer.ipi or '--' }}</td>
          <td><span class="tag tag-full">{{ ww.writer.pro or '--' }}</span></td>
          <td><span style="font-size:13px;font-weight:700;color:var(--a)">{{ "%.2f"|format(ww.writer_percentage) }}%</span></td>
          <td style="color:var(--t2)">{{ ww.publisher or '--' }}</td>
          <td style="font-family:var(--fm);font-size:12px;color:var(--t2)">{{ ww.publisher_ipi or '--' }}</td>
          <td>{% if ww.writer.has_master_contract %}<span class="tag tag-s1">Yes</span>{% else %}<span style="color:var(--t3)">No</span>{% endif %}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#128196;</div><span class="card-title">Generated Documents</span></div>
  <div class="tbl-wrap">
    <table class="tbl tbl-docs">
      <thead>
        <tr>
          <th>Writer</th><th>Type</th><th>File</th><th>Generated At</th>
          <th>DocuSign</th><th>DS Status</th><th>Certificate</th><th>Signed PDF</th><th>Status</th>
        </tr>
      </thead>
      <tbody>
        {% for doc in documents %}
        <tr data-doc-id="{{ doc.id }}">
          <td data-label="Writer" style="font-weight:600">{{ doc.writer_name_snapshot }}</td>
          <td data-label="Type"><span class="tag tag-full">{{ doc.document_type }}</span></td>
          <td data-label="File" data-hide-mobile>
            {% if doc.drive_web_view_link %}
              <a href="{{ doc.drive_web_view_link }}" target="_blank" class="file-link" title="{{ doc.file_name }}">&#128196; {{ doc.file_name | truncate(30,true,'...') }}</a>
            {% else %}
              <span class="file-link-plain">{{ doc.file_name }}</span>
            {% endif %}
          </td>
          <td data-label="Generated" data-hide-mobile style="color:var(--t3);font-size:11.5px">{{ doc.generated_at.strftime('%b %d, %Y') if doc.generated_at else '--' }}</td>
          <td data-label="DocuSign">
            <form method="post" action="/documents/{{ doc.id }}/send-docusign" class="ds-form">
              <button type="submit" class="btn btn-sec btn-xs ds-btn">
                <span class="ds-lbl">{% if doc.docusign_status == 'completed' %}Resend{% elif doc.docusign_status == 'sent' %}Sent{% elif doc.docusign_status == 'delivered' %}Delivered{% else %}Send{% endif %}</span>
                <span class="spin ds-spin"></span>
              </button>
            </form>
          </td>
          <td data-label="DS Status">{% if doc.docusign_status %}<span class="status s-{{ doc.docusign_status }}"><span class="status-dot"></span>{{ doc.docusign_status | title }}</span>{% else %}--{% endif %}</td>
          <td data-label="Certificate" data-hide-mobile>{% if doc.certificate_drive_web_view_link %}<a href="{{ doc.certificate_drive_web_view_link }}" target="_blank" class="btn btn-sec btn-xs">Cert</a>{% else %}--{% endif %}</td>
          <td data-label="Signed PDF">
            {% if doc.signed_pdf_drive_web_view_link %}<a href="{{ doc.signed_pdf_drive_web_view_link }}" target="_blank" class="file-link">&#128209; Signed</a>
            {% elif doc.signed_web_view_link %}<a href="{{ doc.signed_web_view_link }}" target="_blank" class="file-link">&#128209; Signed</a>
            {% else %}--{% endif %}
          </td>
          <td data-label="Status">{% if doc.status %}<span class="status s-{{ doc.status }}"><span class="status-dot"></span>{{ doc.status | replace('_',' ') | title }}</span>{% else %}--{% endif %}</td>
        </tr>
        {% endfor %}
        {% if not documents %}<tr class="empty"><td colspan="9">No documents generated yet.</td></tr>{% endif %}
      </tbody>
    </table>
  </div>
</div>
</div>
</main>
</div>
""" + _SB_JS + """
<script>
document.querySelectorAll('.ds-form').forEach(function(f) {
  f.addEventListener('submit', function(e) {
    e.preventDefault();
    var btn = f.querySelector('.ds-btn');
    var spin = f.querySelector('.ds-spin');
    var lbl = f.querySelector('.ds-lbl');
    if (btn) btn.disabled = true;
    if (spin) spin.classList.add('on');
    if (lbl) lbl.textContent = 'Sending...';
    setTimeout(function() { f.submit(); }, 150);
  });
});
</script>
<div class="action-bar">
  <a href="/works/{{ work.id }}/edit" class="btn btn-primary">Edit Work</a>
  {% if work.batch_id %}<a href="/batches/{{ work.batch_id }}" class="btn btn-sec">View Session</a>{% endif %}
  <form method="post" action="/works/{{ work.id }}/delete" style="display:contents" onsubmit="return confirm('Delete this work? This will remove its writer links and generated documents.');">
    <button type="submit" class="btn btn-danger">Delete</button>
  </form>
  <div class="ab-space"></div>
  <a href="/works" class="btn btn-sec">Back</a>
</div>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""

# ================================================================
# WORK EDIT HTML
# ================================================================

WORK_EDIT_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Edit {{ work.title }} - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("works_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
{% with messages = get_flashed_messages() %}
{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}

<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#9998;</div>
    <div>
      <div class="ph-title">Edit Work</div>
      <div class="ph-sub">{{ work.title }}</div>
    </div>
  </div>
  <div class="ph-actions">
    <a href="/works/{{ work.id }}" class="btn btn-sec btn-sm">Back to Work</a>
  </div>
</div>

<form method="post" id="workEditForm">
  <div class="card">
    <div class="card-hd"><div class="card-ico">&#128395;</div><span class="card-title">Work Information</span></div>
    <div class="card-body">
      <div class="g g2" style="margin-bottom:12px">
        <div class="field">
          <label class="label">Work Title</label>
          <input class="inp" name="title" value="{{ work.title or '' }}" required>
        </div>
        <div class="field">
          <label class="label">Contract Date</label>
          <input class="inp" type="date" name="contract_date" value="{{ work.contract_date.isoformat() if work.contract_date else '' }}" required>
        </div>
      </div>

      <div class="g g2">
        <div class="field">
          <label class="label">Session</label>
          <select class="inp" name="batch_id">
            <option value="">-- No Session --</option>
            {% for batch in batches %}
              <option value="{{ batch.id }}" {% if work.batch_id == batch.id %}selected{% endif %}>
                Session #{{ batch.id }}{% if batch.session_name %} -- {{ batch.session_name }}{% endif %} -- {{ batch.contract_date.strftime('%Y-%m-%d') }}
              </option>
            {% endfor %}
          </select>
        </div>
        <div class="field">
          <label class="label">Current Session</label>
          <input class="inp" value="{% if work.batch and work.batch.session_name %}{{ work.batch.session_name }}{% elif work.batch_id %}Session #{{ work.batch_id }}{% else %}--{% endif %}" disabled>
        </div>
      </div>
    </div>
  </div>

  <div class="card">
    <div class="card-hd">
      <div class="card-ico">&#128101;</div>
      <span class="card-title">Work Writers</span>
      <div class="card-actions">
        <button type="button" class="btn btn-sec btn-sm" onclick="addExistingWriterRow()">+ Add Existing Writer</button>
      </div>
    </div>
    <div class="card-body">
      <div class="tbl-wrap">
        <table class="tbl tbl-edit-writers">
          <thead>
            <tr>
              <th>Writer</th>
              <th>IPI</th>
              <th>PRO</th>
              <th>Split %</th>
              <th>Publisher</th>
              <th>Publisher IPI</th>
              <th></th>
            </tr>
          </thead>
          <tbody id="workWriterTableBody">
            {% for ww in work.work_writers %}
            <tr class="work-writer-row existing-row">
              <td style="font-weight:600">
                {% if ww.writer %}
                  <button
                    type="button"
                    class="btn btn-sec btn-sm"
                    style="padding:4px 8px"
                    onclick="openWriterModal({{ ww.writer.id }})">
                    {{ ww.writer.full_name }}
                  </button>
                {% else %}
                  --
                {% endif %}
                <input type="hidden" name="work_writer_id" value="{{ ww.id }}">
                <input type="hidden" name="existing_writer_id" value="{{ ww.writer.id if ww.writer else '' }}">
              </td>
              <td style="font-family:var(--fm);font-size:12px;color:var(--t2)">
                {{ ww.writer.ipi if ww.writer and ww.writer.ipi else '--' }}
              </td>
              <td>
                <span class="tag tag-full">{{ ww.writer.pro if ww.writer and ww.writer.pro else '--' }}</span>
              </td>
              <td>
                <input class="inp split-inp" type="number" step="0.01" min="0" max="100" name="writer_percentage" value="{{ ww.writer_percentage or 0 }}">
              </td>
              <td>
                <input class="inp" name="publisher" value="{{ ww.publisher or '' }}">
              </td>
              <td>
                <input class="inp" name="publisher_ipi" value="{{ ww.publisher_ipi or '' }}">
              </td>
              <td>
                <button type="button" class="btn btn-danger btn-sm" onclick="removeWorkWriterRow(this)">Remove</button>
              </td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>

      <div style="margin-top:12px;display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
        <div style="color:var(--t2);font-size:12px">
          Total split must equal 100%.
        </div>
        <div style="font-family:var(--fm);font-size:12px;color:var(--t1)">
          Total: <span id="editSplitTotal">0.00</span>%
        </div>
      </div>
    </div>
  </div>

  <div class="ph-actions" style="justify-content:flex-end">
    <button type="submit" class="btn btn-primary">Save Changes</button>
  </div>
</form>
</div>
</main>
</div>

<div id="writerEditModal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:9999;">
  <div id="writerEditPanel" style="position:absolute;top:5%;left:50%;transform:translateX(-50%);width:92%;max-width:900px;height:88%;background:#0f172a;border:1px solid rgba(255,255,255,.12);border-radius:14px;overflow:hidden;display:flex;flex-direction:column;box-shadow:0 20px 60px rgba(0,0,0,.45);">
    
    <div style="padding:12px 16px;background:#111827;color:#fff;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid rgba(255,255,255,.08);">
      <span style="font-weight:600;">Edit Writer</span>
      <button type="button" onclick="closeWriterModal()" style="background:none;border:none;color:#fff;font-size:20px;cursor:pointer;">×</button>
    </div>

    <div id="writerModalBody" style="padding:18px;overflow:auto;flex:1;">
      <div style="color:white">Loading writer...</div>
    </div>

  </div>
</div>

""" + _SB_JS + """
<script>
function recalcEditSplit() {
  var total = 0;
  document.querySelectorAll('.split-inp').forEach(function(inp) {
    total += parseFloat(inp.value || 0) || 0;
  });
  var el = document.getElementById('editSplitTotal');
  if (el) el.textContent = total.toFixed(2);
}

function removeWorkWriterRow(btn) {
  var row = btn.closest('tr');
  if (row) row.remove();

  var rows = document.querySelectorAll('#workWriterTableBody tr');
  if (rows.length === 1) {
    var onlySplit = rows[0].querySelector('.split-inp');
    if (onlySplit) {
      onlySplit.value = '100.00';
    }
  }

  recalcEditSplit();
}

function addExistingWriterRow() {
  var tbody = document.getElementById('workWriterTableBody');
  var tr = document.createElement('tr');
  tr.className = 'work-writer-row new-row';
  tr.innerHTML = `
    <td>
      <div class="ac-wrap" style="min-width:220px">
        <input class="inp new-writer-search" type="text" placeholder="Search existing writer...">
        <div class="ac-box new-writer-sug"></div>
      </div>
      <input type="hidden" name="work_writer_id" value="">
      <input type="hidden" name="existing_writer_id" value="">
      <div class="new-writer-name" style="margin-top:6px;font-size:12px;color:var(--t2)">No writer selected</div>
    </td>
    <td class="new-writer-ipi" style="font-family:var(--fm);font-size:12px;color:var(--t2)">--</td>
    <td class="new-writer-pro"><span class="tag tag-full">--</span></td>
    <td><input class="inp split-inp" type="number" step="0.01" min="0" max="100" name="writer_percentage" value="0"></td>
    <td><input class="inp" name="publisher" value=""></td>
    <td><input class="inp" name="publisher_ipi" value=""></td>
    <td><button type="button" class="btn btn-danger btn-sm" onclick="removeWorkWriterRow(this)">Remove</button></td>
  `;
  tbody.appendChild(tr);

  var searchInp = tr.querySelector('.new-writer-search');
  var sug = tr.querySelector('.new-writer-sug');
  var hiddenWriterId = tr.querySelector('input[name="existing_writer_id"]');
  var displayName = tr.querySelector('.new-writer-name');
  var ipiCell = tr.querySelector('.new-writer-ipi');
  var proCell = tr.querySelector('.new-writer-pro');

  searchInp.addEventListener('input', function() {
    var q = (searchInp.value || '').trim();
    if (q.length < 2) {
      sug.style.display = 'none';
      sug.innerHTML = '';
      return;
    }

    fetch('/writers/search?q=' + encodeURIComponent(q))
      .then(function(res) { return res.json(); })
      .then(function(ws) {
        if (!ws.length) {
          sug.style.display = 'none';
          sug.innerHTML = '';
          return;
        }

        sug.innerHTML = ws.map(function(w) {
          var safe = JSON.stringify(w).replace(/'/g, "&#39;");
          return "<div class='ac-item' data-w='" + safe + "'>" +
            "<strong>" + w.full_name + "</strong><br>" +
            "<small>" + (w.ipi || '--') + "</small>" +
            "</div>";
        }).join('');
        sug.style.display = 'block';

        sug.querySelectorAll('.ac-item').forEach(function(item) {
          item.addEventListener('click', function() {
            var w = JSON.parse(item.dataset.w);
            hiddenWriterId.value = w.id || '';
            displayName.textContent = w.full_name || 'No writer selected';
            ipiCell.textContent = w.ipi || '--';
            proCell.innerHTML = "<span class='tag tag-full'>" + (w.pro || '--') + "</span>";
            tr.querySelector('input[name="publisher"]').value = w.default_publisher || '';
            tr.querySelector('input[name="publisher_ipi"]').value = w.default_publisher_ipi || '';
            searchInp.value = '';
            sug.style.display = 'none';
            sug.innerHTML = '';
          });
        });
      });
  });

  document.addEventListener('click', function(e) {
    if (!searchInp.contains(e.target) && !sug.contains(e.target)) {
      sug.style.display = 'none';
    }
  });

  var splitInp = tr.querySelector('.split-inp');
  splitInp.addEventListener('input', recalcEditSplit);
  recalcEditSplit();
}

document.addEventListener('DOMContentLoaded', function() {
  document.querySelectorAll('.split-inp').forEach(function(inp) {
    inp.addEventListener('input', recalcEditSplit);
  });
  recalcEditSplit();
});

function openWriterModal(writerId) {
  var modal = document.getElementById('writerEditModal');
  var body = document.getElementById('writerModalBody');

  modal.style.display = 'block';
  document.body.style.overflow = 'hidden';
  body.innerHTML = '<div style="color:white">Loading writer...</div>';

  fetch('/writers/' + writerId + '/modal')
    .then(function(res) { return res.text(); })
    .then(function(html) {
      body.innerHTML = html;
    })
    .catch(function() {
      body.innerHTML = '<div style="color:#ff8a8a">Failed to load writer.</div>';
    });
}

function closeWriterModal() {
  document.getElementById('writerEditModal').style.display = 'none';
  document.getElementById('writerModalBody').innerHTML = '<div style="color:white">Loading writer...</div>';
  document.body.style.overflow = 'auto';
}

function saveWriterModal(e, writerId) {
  e.preventDefault();

  var form = document.getElementById('writerModalForm');
  var err = document.getElementById('writerModalError');
  err.textContent = '';

  var fd = new FormData(form);

  fetch('/writers/' + writerId + '/modal-save', {
    method: 'POST',
    body: fd
  })
  .then(function(res) { return res.json(); })
  .then(function(data) {
    if (!data.ok) {
      err.textContent = data.error || 'Failed to save writer.';
      return;
    }

    closeWriterModal();
    refreshWriterRow(writerId, data.writer);
  })
  .catch(function() {
    err.textContent = 'Failed to save writer.';
  });
}

function refreshWriterRow(writerId, writerData) {
  document.querySelectorAll('#workWriterTableBody tr').forEach(function(row) {
    var hiddenWriterId = row.querySelector('input[name="existing_writer_id"]');
    if (!hiddenWriterId) return;
    if (String(hiddenWriterId.value) !== String(writerId)) return;

    var writerBtn = row.querySelector('td button');
    if (writerBtn && writerData.full_name) {
      writerBtn.textContent = writerData.full_name;
    }

    var cells = row.querySelectorAll('td');

    if (cells[1]) {
      cells[1].textContent = writerData.ipi || '--';
    }

    if (cells[2]) {
      var pro = writerData.pro || '--';
      cells[2].innerHTML = '<span class="tag tag-full">' + pro + '</span>';
    }

    var publisherInput = row.querySelector('input[name="publisher"]');
    if (publisherInput) {
      publisherInput.value = writerData.default_publisher || '';
    }

    var publisherIpiInput = row.querySelector('input[name="publisher_ipi"]');
    if (publisherIpiInput) {
      publisherIpiInput.value = writerData.default_publisher_ipi || '';
    }
  });
}


document.addEventListener('click', function(e) {
  var modal = document.getElementById('writerEditModal');
  if (e.target === modal) {
    closeWriterModal();
  }
});

function syncModalPro(sel) {
  var pro = sel.value || '';
  var form = sel.closest('form');
  if (!form) return;

  var publisherMap = {
    BMI:   { name: 'Songs of Afinarte',    ipi: '817874992' },
    ASCAP: { name: 'Melodies of Afinarte', ipi: '807953316' },
    SESAC: { name: 'Music of Afinarte',    ipi: '817094629' }
  };

  var p = publisherMap[pro];
  if (!p) return;

  var publisherInp = form.querySelector('input[name="default_publisher"]');
  var publisherIpiInp = form.querySelector('input[name="default_publisher_ipi"]');

  if (publisherInp) publisherInp.value = p.name;
  if (publisherIpiInp) publisherIpiInp.value = p.ipi;
}

</script>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""

# ================================================================
# WRITER DIRECTORY
# ================================================================

WRITERS_LIST_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Writers - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("writers_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128101;</div>
    <div>
      <div class="ph-title">Writer Directory</div>
      <div class="ph-sub">Search and review all writers in the system</div>
    </div>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#128269;</div><span class="card-title">Search</span></div>
  <div class="card-body">
    <form method="get" style="display:flex;gap:8px;flex-wrap:wrap">
      <input class="inp" name="q" value="{{ q }}" placeholder="Search by name, IPI, email, or phone..." style="max-width:420px">
      <button class="btn btn-sec" type="submit">Search</button>
      {% if q %}<a href="/writers" class="btn btn-sec">Clear</a>{% endif %}
    </form>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#128203;</div><span class="card-title">All Writers</span></div>
  <div class="tbl-wrap">
    <table class="tbl" style="table-layout:auto">
      <thead>
        <tr>
          <th style="width:35%">Writer</th>
          <th>IPI</th>
          <th>PRO</th>
          <th>Works</th>
        </tr>
      </thead>
      <tbody>
        {% for item in writers %}
        <tr class="wr-row" data-writer="{{ item.writer.id }}" onclick="toggleWriter({{ item.writer.id }})">
          <td>
            <span class="expand-chevron">&#9658;</span>
            <span style="font-weight:600">{{ item.writer.full_name }}</span>
            {% if item.writer.writer_aka %}<div style="font-size:11px;color:var(--t3);margin-top:2px;margin-left:16px">aka {{ item.writer.writer_aka }}</div>{% endif %}
          </td>
          <td style="font-family:var(--fm);font-size:12px;color:var(--t2)">{{ item.writer.ipi or '--' }}</td>
          <td><span class="tag tag-full">{{ item.writer.pro or '--' }}</span></td>
          <td><span style="background:rgba(99,133,255,.1);color:var(--a);border:1px solid rgba(99,133,255,.2);border-radius:99px;padding:2px 8px;font-size:11px;font-weight:700">{{ item.work_count }}</span></td>
        </tr>
        <tr class="wr-detail-row" id="wdetail-{{ item.writer.id }}">
          <td colspan="4">
            <div class="wr-detail-inner">
              <div class="wd-section">
                <div class="wd-label">Contact</div>
                <div style="font-size:13px;display:flex;flex-direction:column;gap:4px">
                  {% if item.writer.email %}<span>&#9993; {{ item.writer.email }}</span>{% endif %}
                  {% if item.writer.phone_number %}<span>&#128222; {{ item.writer.phone_number }}</span>{% endif %}
                  {% if item.writer.address %}
                  <span style="color:var(--t3);font-size:12px">{{ item.writer.address }}{% if item.writer.city %}, {{ item.writer.city }}{% endif %}{% if item.writer.state %}, {{ item.writer.state }}{% endif %}{% if item.writer.zip_code %} {{ item.writer.zip_code }}{% endif %}</span>
                  {% endif %}
                  {% if not item.writer.email and not item.writer.phone_number %}<span style="color:var(--t3)">No contact info</span>{% endif %}
                </div>
                <div class="wd-label" style="margin-top:12px">Publishing Contract</div>
                {% if item.writer.has_master_contract %}<span class="tag tag-s1">Yes</span>{% else %}<span style="color:var(--t3);font-size:13px">No</span>{% endif %}
              </div>
              <div class="wd-section">
                <div class="wd-label">Details</div>
                <div style="font-size:13px;display:flex;flex-direction:column;gap:4px;color:var(--t2)">
                  {% if item.writer.ipi %}<span>IPI: <span style="font-family:var(--fm)">{{ item.writer.ipi }}</span></span>{% endif %}
                  {% if item.writer.pro %}<span>PRO: {{ item.writer.pro }}</span>{% endif %}
                  <span style="color:var(--t3);font-size:11px">Added {{ item.writer.created_at.strftime('%b %d, %Y') if item.writer.created_at else '--' }}</span>
                </div>
                <div style="display:flex;gap:8px;margin-top:16px">
                  <a href="/writers/{{ item.writer.id }}/edit" class="btn btn-primary btn-sm" style="color:#fff" onclick="event.stopPropagation()">Edit</a>
                  <a href="/writers/{{ item.writer.id }}" class="btn btn-sec btn-sm" onclick="event.stopPropagation()">Full View</a>
                </div>
              </div>
            </div>
          </td>
        </tr>
        {% endfor %}
        {% if not writers %}
          <tr class="empty"><td colspan="4">No writers found{% if q %} for "{{ q }}"{% endif %}.</td></tr>
        {% endif %}
      </tbody>
    </table>
  </div>
  {% if pagination.pages > 1 %}
  <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-top:1px solid var(--b1);font-size:13px;color:var(--t2)">
    <span>{{ pagination.total }} writers &mdash; page {{ pagination.page }} of {{ pagination.pages }}</span>
    <div style="display:flex;gap:6px">
      {% if pagination.has_prev %}<a href="?q={{ q }}&page={{ pagination.prev_num }}" class="btn btn-sec btn-sm">&#8592; Prev</a>{% endif %}
      {% if pagination.has_next %}<a href="?q={{ q }}&page={{ pagination.next_num }}" class="btn btn-sec btn-sm">Next &#8594;</a>{% endif %}
    </div>
  </div>
  {% endif %}
</div>
</div>
</main>
</div>
""" + _SB_JS + """
<script>
function toggleWriter(id) {
  var row = document.getElementById('wdetail-' + id);
  var header = document.querySelector('[data-writer="' + id + '"]');
  var isOpen = row.classList.contains('open');
  document.querySelectorAll('.wr-detail-row.open').forEach(function(r){ r.classList.remove('open'); });
  document.querySelectorAll('.wr-row.open').forEach(function(r){ r.classList.remove('open'); });
  if (!isOpen) {
    row.classList.add('open');
    header.classList.add('open');
    row.scrollIntoView({behavior:'smooth', block:'nearest'});
  }
}
</script>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""


# ================================================================
# WRITER PROFILE HTML
# ================================================================

WRITER_DETAIL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ writer.full_name }} - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("writers_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128101;</div>
    <div>
      <div class="ph-title">{{ writer.full_name }}</div>
      <div class="ph-sub">{{ writer.writer_aka or 'Writer profile' }}</div>
    </div>
  </div>
  <div class="ph-actions">
    <a href="/writers/{{ writer.id }}/edit" class="btn btn-primary btn-sm">Edit Writer</a>
    <a href="/writers" class="btn btn-sec btn-sm">Back</a>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#9997;</div><span class="card-title">Writer Info</span></div>
  <div class="card-body">
    <div class="g g4" style="gap:8px 22px;margin-bottom:10px">
      <div class="info-item"><label>First Name</label><span>{{ writer.first_name or '--' }}</span></div>
      <div class="info-item"><label>Middle Name</label><span>{{ writer.middle_name or '--' }}</span></div>
      <div class="info-item"><label>Last Name(s)</label><span>{{ writer.last_names or '--' }}</span></div>
      <div class="info-item"><label>AKA / Stage</label><span>{{ writer.writer_aka or '--' }}</span></div>
    </div>
    <div class="g g4" style="gap:8px 22px;margin-bottom:10px">
      <div class="info-item"><label>Address</label><span>{{ writer.address or '--' }}</span></div>
      <div class="info-item"><label>City</label><span>{{ writer.city or '--' }}</span></div>
      <div class="info-item"><label>State</label><span>{{ writer.state or '--' }}</span></div>
      <div class="info-item"><label>Zip</label><span>{{ writer.zip_code or '--' }}</span></div>
    </div>
    <div class="g g4" style="gap:8px 22px;margin-bottom:10px">
      <div class="info-item"><label>Email</label><span>{{ writer.email or '--' }}</span></div>
      <div class="info-item"><label>Phone</label><span>{{ writer.phone_number or '--' }}</span></div>
      <div class="info-item"><label>IPI</label><span>{{ writer.ipi or '--' }}</span></div>
      <div class="info-item"><label>PRO</label><span>{{ writer.pro or '--' }}</span></div>
    </div>
    <div class="g g2" style="gap:8px 22px">
      <div class="info-item"><label>Publishing Contract</label>
        <span>{% if writer.has_master_contract %}<span class="tag tag-s1">Yes</span>{% else %}No{% endif %}</span>
      </div>
      <div class="info-item"><label>Created</label><span>{{ writer.created_at.strftime('%b %d, %Y %H:%M') if writer.created_at else '--' }}</span></div>
    </div>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#128395;</div><span class="card-title">Works</span></div>
  <div class="tbl-wrap">
    <table class="tbl tbl-writer-works" style="table-layout:auto">
      <thead>
        <tr>
          <th>Work Title</th>
          <th>Split %</th>
          <th>Publisher</th>
          <th>Session</th>
          <th>Date</th>
        </tr>
      </thead>
      <tbody>
        {% for ww in work_writers %}
        <tr class="wk-row" data-wk="{{ ww.id }}" onclick="toggleWk({{ ww.id }})">
          <td>
            <span class="expand-chevron">&#9658;</span>
            <span style="font-weight:600">{{ ww.work.title if ww.work else '--' }}</span>
          </td>
          <td><span style="font-size:13px;font-weight:700;color:var(--a)">{{ "%.2f"|format(ww.writer_percentage or 0) }}%</span></td>
          <td style="color:var(--t2)">{{ ww.publisher or '--' }}</td>
          <td style="color:var(--t2)">
            {% if ww.work and ww.work.batch_id %}
              <a href="/batches/{{ ww.work.batch_id }}" style="color:var(--a)" onclick="event.stopPropagation()">
                {% if ww.work.batch and ww.work.batch.session_name %}{{ ww.work.batch.session_name }}{% else %}Session #{{ ww.work.batch_id }}{% endif %}
              </a>
            {% else %}--{% endif %}
          </td>
          <td style="color:var(--t2);font-size:12px;white-space:nowrap">{{ ww.work.contract_date.strftime('%b %d, %Y') if ww.work and ww.work.contract_date else '--' }}</td>
        </tr>
        <tr class="wk-detail-row" id="wkdetail-{{ ww.id }}">
          <td colspan="5">
            <div class="wk-detail-inner">
              <div class="wd-section">
                <div class="wd-label">Publisher</div>
                <div style="font-size:13px;color:var(--t2)">{{ ww.publisher or '--' }}</div>
                {% if ww.publisher_ipi %}
                <div style="font-size:12px;color:var(--t3);font-family:var(--fm)">IPI: {{ ww.publisher_ipi }}</div>
                {% endif %}
              </div>
              <div class="wd-section">
                <div class="wd-label">Session</div>
                <div style="font-size:13px;color:var(--t2)">
                  {% if ww.work and ww.work.batch_id %}
                    <a href="/batches/{{ ww.work.batch_id }}" style="color:var(--a)">{{ ww.work.batch.session_name if ww.work.batch and ww.work.batch.session_name else 'Session #' ~ ww.work.batch_id }}</a>
                  {% else %}--{% endif %}
                </div>
                <div style="margin-top:14px">
                  {% if ww.work %}
                  <a href="/works/{{ ww.work.id }}/edit" class="btn btn-primary btn-sm" style="color:#fff" onclick="event.stopPropagation()">Edit Work</a>
                  <a href="/works/{{ ww.work.id }}" class="btn btn-sec btn-sm" style="margin-left:6px" onclick="event.stopPropagation()">Full View</a>
                  {% endif %}
                </div>
              </div>
            </div>
          </td>
        </tr>
        {% endfor %}
        {% if not work_writers %}
          <tr class="empty"><td colspan="5">No works found for this writer.</td></tr>
        {% endif %}
      </tbody>
    </table>
  </div>
</div>
</div>
</main>
</div>
""" + _SB_JS + """
<script>
function toggleWk(id) {
  var row = document.getElementById('wkdetail-' + id);
  var header = document.querySelector('[data-wk="' + id + '"]');
  var isOpen = row.classList.contains('open');
  document.querySelectorAll('.wk-detail-row.open').forEach(function(r){ r.classList.remove('open'); });
  document.querySelectorAll('.wk-row.open').forEach(function(r){ r.classList.remove('open'); });
  if (!isOpen) {
    row.classList.add('open');
    header.classList.add('open');
    row.scrollIntoView({behavior:'smooth', block:'nearest'});
  }
}
</script>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""

# ================================================================
# WRITER EDIT HTML
# ================================================================

WRITER_EDIT_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Edit {{ writer.full_name }} - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("writers_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
{% with messages = get_flashed_messages() %}
{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}

<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#9998;</div>
    <div>
      <div class="ph-title">Edit Writer</div>
      <div class="ph-sub">{{ writer.full_name }}</div>
    </div>
  </div>
  <div class="ph-actions">
    <a href="/writers/{{ writer.id }}" class="btn btn-sec btn-sm">Back to Profile</a>
  </div>
</div>

<form method="post">

  <div class="card">
    <div class="card-hd">
      <div class="card-ico">&#128101;</div>
      <span class="card-title">Writer Information</span>
    </div>

    <div class="card-body">

      <div class="g g4" style="margin-bottom:12px">
        <div class="field">
          <label class="label">First Name</label>
          <input class="inp" name="first_name" value="{{ writer.first_name or '' }}">
        </div>

        <div class="field">
          <label class="label">Middle Name</label>
          <input class="inp" name="middle_name" value="{{ writer.middle_name or '' }}">
        </div>

        <div class="field">
          <label class="label">Last Name(s)</label>
          <input class="inp" name="last_names" value="{{ writer.last_names or '' }}">
        </div>

        <div class="field">
          <label class="label">AKA / Stage</label>
          <input class="inp" name="writer_aka" value="{{ writer.writer_aka or '' }}">
        </div>
      </div>

      <div class="g g2" style="margin-bottom:12px">
        <div class="field">
          <label class="label">Email</label>
          <input class="inp" name="email" type="email" value="{{ writer.email or '' }}">
        </div>

        <div class="field">
          <label class="label">Phone Number</label>
          <input class="inp" name="phone_number" value="{{ writer.phone_number or '' }}">
        </div>
      </div>

      <div class="g g3" style="margin-bottom:12px">
        <div class="field">
          <label class="label">IPI</label>
          <input class="inp" name="ipi" value="{{ writer.ipi or '' }}">
        </div>

        <div class="field">
          <label class="label">PRO</label>
          <select class="inp" name="pro" onchange="syncWriterModalPro(this)">
            <option value="">Select PRO</option>
            <option value="BMI" {% if writer.pro == 'BMI' %}selected{% endif %}>BMI</option>
            <option value="ASCAP" {% if writer.pro == 'ASCAP' %}selected{% endif %}>ASCAP</option>
            <option value="SESAC" {% if writer.pro == 'SESAC' %}selected{% endif %}>SESAC</option>
          </select>
        </div>

        <div class="field">
          <label class="label">Publishing Contract</label>
          <select class="inp" name="has_master_contract">
            <option value="0" {% if not writer.has_master_contract %}selected{% endif %}>No</option>
            <option value="1" {% if writer.has_master_contract %}selected{% endif %}>Yes</option>
          </select>
        </div>
      </div>

      <div class="g g2" style="margin-bottom:12px">
        <div class="field">
          <label class="label">Default Publisher</label>
          <input class="inp" name="default_publisher"
                 value="{{ writer.default_publisher or default_publisher_for_pro(writer.pro) }}">
        </div>

        <div class="field">
          <label class="label">Default Publisher IPI</label>
          <input class="inp" name="default_publisher_ipi"
                 value="{{ writer.default_publisher_ipi or default_publisher_ipi_for_pro(writer.pro) }}">
        </div>
      </div>

      <div class="g g4a">
        <div class="field">
          <label class="label">Street</label>
          <input class="inp" name="address" value="{{ writer.address or '' }}">
        </div>

        <div class="field">
          <label class="label">City</label>
          <input class="inp" name="city" value="{{ writer.city or '' }}">
        </div>

        <div class="field">
          <label class="label">State</label>
          <input class="inp" name="state" value="{{ writer.state or '' }}">
        </div>

        <div class="field">
          <label class="label">Zip</label>
          <input class="inp" name="zip_code" value="{{ writer.zip_code or '' }}">
        </div>
      </div>

    </div>
  </div>

  <div class="ph-actions" style="justify-content:flex-end">
    <button type="submit" class="btn btn-primary">Save Changes</button>
  </div>

</form>
</div>
</main>
</div>
""" + _SB_JS + """
<script>
function syncWriterModalPro(sel) {
  var pro = sel.value || '';
  var form = sel.closest('form');
  if (!form) return;

  var publisherMap = {
    BMI:   { name: 'Songs of Afinarte',    ipi: '817874992' },
    ASCAP: { name: 'Melodies of Afinarte', ipi: '807953316' },
    SESAC: { name: 'Music of Afinarte',    ipi: '817094629' }
  };

  var p = publisherMap[pro];
  if (!p) return;

  var publisherInp = form.querySelector('input[name="default_publisher"]');
  var publisherIpiInp = form.querySelector('input[name="default_publisher_ipi"]');

  if (publisherInp) publisherInp.value = p.name;
  if (publisherIpiInp) publisherIpiInp.value = p.ipi;
}
</script>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""


# ================================================================
# ADMIN HTML
# ================================================================

ADMIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Admin - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("admin") + """
<main class="main">
""" + _topbar() + """
<div class="page">

{% with messages = get_flashed_messages() %}
{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}

<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128736;</div>
    <div>
      <div class="ph-title">Admin Panel</div>
      <div class="ph-sub">Import catalog, manage data, and run admin tools.</div>
    </div>
  </div>
</div>


<div class="card">
  <div class="card-hd">
    <div class="card-ico">&#128191;</div>
    <span class="card-title">Import Releases &amp; Tracks Catalog</span>
  </div>
  <div class="card-body">
    <p style="color:var(--t2);font-size:13px;margin-bottom:14px">
      Import your full catalog CSV (one row per track) with releases, artists, and publishing works.
      Rows with <strong>Publishing = TRUE</strong> will also create songwriting works.
    </p>
    <a href="/admin/import-catalog-csv" class="btn btn-primary">Go to Catalog Import</a>
  </div>
</div>

<div class="card">
  <div class="card-hd">
    <div class="card-ico">&#128257;</div>
    <span class="card-title">Merge Duplicate Writers</span>
  </div>
  <div class="card-body">
    <p style="color:var(--t2);font-size:13px;margin-bottom:16px">
      Search for two writers and merge them. All works and documents from the
      <strong>duplicate</strong> will be moved to the <strong>primary</strong>, then the duplicate is deleted.
    </p>
    <form method="post" action="/admin/merge-writers" onsubmit="return confirm('Merge these two writers? This cannot be undone.')">
      <div class="g g2" style="margin-bottom:14px">
        <div class="field">
          <label class="label">Primary Writer (keep this one)</label>
          <input class="inp" type="text" id="mergeSearch1" placeholder="Search by name..." autocomplete="off">
          <input type="hidden" name="primary_writer_id" id="primaryWriterId" required>
          <div id="mergeDrop1" style="display:none;position:relative;z-index:100;background:var(--bg3);border:1px solid var(--b0);border-radius:var(--rs);margin-top:2px;max-height:180px;overflow-y:auto"></div>
          <div id="primaryLabel" style="font-size:12px;color:var(--ag);margin-top:4px"></div>
        </div>
        <div class="field">
          <label class="label">Duplicate Writer (delete this one)</label>
          <input class="inp" type="text" id="mergeSearch2" placeholder="Search by name..." autocomplete="off">
          <input type="hidden" name="duplicate_writer_id" id="duplicateWriterId" required>
          <div id="mergeDrop2" style="display:none;position:relative;z-index:100;background:var(--bg3);border:1px solid var(--b0);border-radius:var(--rs);margin-top:2px;max-height:180px;overflow-y:auto"></div>
          <div id="duplicateLabel" style="font-size:12px;color:var(--ar);margin-top:4px"></div>
        </div>
      </div>
      <button type="submit" class="btn btn-danger" id="mergBtn" disabled>Merge Writers</button>
    </form>
  </div>
</div>

</div>
</main>
</div>
""" + _SB_JS + """
<script>
function mergeSearch(inputId, dropId, hiddenId, labelId, labelColor) {
  var inp = document.getElementById(inputId);
  var drop = document.getElementById(dropId);
  var hidden = document.getElementById(hiddenId);
  var label = document.getElementById(labelId);
  inp.addEventListener('input', function() {
    var q = inp.value.trim();
    hidden.value = '';
    label.textContent = '';
    checkMergeReady();
    if (q.length < 2) { drop.style.display = 'none'; return; }
    fetch('/writers/search?q=' + encodeURIComponent(q))
      .then(function(r){ return r.json(); })
      .then(function(results) {
        drop.innerHTML = '';
        if (!results.length) { drop.style.display = 'none'; return; }
        results.forEach(function(w) {
          var d = document.createElement('div');
          d.style.cssText = 'padding:9px 12px;cursor:pointer;font-size:13px;border-bottom:1px solid var(--b1)';
          d.innerHTML = '<strong>' + w.full_name + '</strong><span style="color:var(--t3);font-size:11px;margin-left:8px">IPI: ' + (w.ipi || '--') + '</span>';
          d.addEventListener('mousedown', function(e) {
            e.preventDefault();
            inp.value = w.full_name;
            hidden.value = w.id;
            label.textContent = '\u2713 ' + w.full_name + (w.ipi ? ' (' + w.ipi + ')' : '');
            label.style.color = labelColor;
            drop.style.display = 'none';
            checkMergeReady();
          });
          drop.appendChild(d);
        });
        drop.style.display = 'block';
      });
  });
  inp.addEventListener('blur', function() { setTimeout(function(){ drop.style.display='none'; }, 150); });
}
function checkMergeReady() {
  var p = document.getElementById('primaryWriterId').value;
  var d = document.getElementById('duplicateWriterId').value;
  var btn = document.getElementById('mergBtn');
  btn.disabled = !(p && d && p !== d);
}
mergeSearch('mergeSearch1', 'mergeDrop1', 'primaryWriterId', 'primaryLabel', 'var(--ag)');
mergeSearch('mergeSearch2', 'mergeDrop2', 'duplicateWriterId', 'duplicateLabel', 'var(--ar)');
</script>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""

# ================================================================
# IMPORT PREVIEW HTML
# ================================================================

IMPORT_PREVIEW_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Import Preview - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("admin") + """
<main class="main">
""" + _topbar() + """
<div class="page">

{% with messages = get_flashed_messages() %}
{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}

<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128065;</div>
    <div>
      <div class="ph-title">Import Preview</div>
      <div class="ph-sub">Review rows before importing them into the catalog.</div>
    </div>
  </div>
  <div class="ph-actions">
    <a href="/admin" class="btn btn-sec btn-sm">Back</a>
  </div>
</div>

<div class="card">
  <div class="card-hd">
    <div class="card-ico">&#128202;</div>
    <span class="card-title">Preview Summary</span>
  </div>
  <div class="card-body">
    <div class="info-grid">
      <div class="info-item"><label>Total Rows</label><span>{{ rows|length }}</span></div>
      <div class="info-item"><label>Valid Rows</label><span>{{ valid_count }}</span></div>
      <div class="info-item"><label>Error Rows</label><span>{{ error_count }}</span></div>
    </div>
  </div>
</div>

<div class="card">
  <div class="card-hd">
    <div class="card-ico">&#128203;</div>
    <span class="card-title">Rows</span>
  </div>
  <div class="tbl-wrap">
    <table class="tbl" style="min-width:1200px">
      <thead>
        <tr>
          <th>#</th>
          <th>Status</th>
          <th>Work</th>
          <th>Contract Date</th>
          <th>Session</th>
          <th>Writer</th>
          <th>IPI</th>
          <th>PRO</th>
          <th>Split %</th>
          <th>Publisher</th>
          <th>Publisher IPI</th>
          <th>Error</th>
        </tr>
      </thead>
      <tbody>
        {% for row in rows %}
        <tr>
          <td>{{ row.row_num }}</td>
          <td>
            {% if row.error %}
              <span class="status s-delivered"><span class="status-dot"></span>Error</span>
            {% else %}
              <span class="status s-completed"><span class="status-dot"></span>Ready</span>
            {% endif %}
          </td>
          <td>{{ row.work_title }}</td>
          <td>{{ row.contract_date }}</td>
          <td>{{ row.session_name }}</td>
          <td>{{ row.writer_full_name }}</td>
          <td>{{ row.ipi or '--' }}</td>
          <td>{{ row.pro or '--' }}</td>
          <td>{{ row.writer_percentage or '--' }}</td>
          <td>{{ row.publisher or '--' }}</td>
          <td>{{ row.publisher_ipi or '--' }}</td>
          <td style="color:#ff8a8a">{{ row.error or '--' }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</div>

<form method="post" action="/admin/import-catalog/confirm">
  <input type="hidden" name="import_token" value="{{ import_token }}">
  <div class="ph-actions" style="justify-content:flex-end">
    <button type="submit" class="btn btn-primary" {% if error_count > 0 %}disabled{% endif %}>Confirm Import</button>
  </div>
</form>

</div>
</main>
</div>
""" + _SB_JS + """
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""

# ================================================================
# WRITER MODAL
# ================================================================

WRITER_MODAL_HTML = """
<form id="writerModalForm" onsubmit="saveWriterModal(event, {{ writer.id }})">
  <div class="g g4" style="margin-bottom:12px">
    <div class="field">
      <label class="label">First Name</label>
      <input class="inp" name="first_name" value="{{ writer.first_name or '' }}">
    </div>
    <div class="field">
      <label class="label">Middle Name</label>
      <input class="inp" name="middle_name" value="{{ writer.middle_name or '' }}">
    </div>
    <div class="field">
      <label class="label">Last Name(s)</label>
      <input class="inp" name="last_names" value="{{ writer.last_names or '' }}">
    </div>
    <div class="field">
      <label class="label">AKA / Stage</label>
      <input class="inp" name="writer_aka" value="{{ writer.writer_aka or '' }}">
    </div>
  </div>

  <div class="g g2" style="margin-bottom:12px">
    <div class="field">
      <label class="label">Email</label>
      <input class="inp" name="email" type="email" value="{{ writer.email or '' }}">
    </div>
    <div class="field">
      <label class="label">Phone Number</label>
      <input class="inp" name="phone_number" value="{{ writer.phone_number or '' }}">
    </div>
  </div>

  <div class="g g3" style="margin-bottom:12px">
    <div class="field">
      <label class="label">IPI</label>
      <input class="inp" name="ipi" value="{{ writer.ipi or '' }}">
    </div>
    <div class="field">
      <label class="label">PRO</label>
      <select class="inp" name="pro" onchange="syncModalPro(this)">
        <option value="">Select PRO</option>
        <option value="BMI" {% if writer.pro == 'BMI' %}selected{% endif %}>BMI</option>
        <option value="ASCAP" {% if writer.pro == 'ASCAP' %}selected{% endif %}>ASCAP</option>
        <option value="SESAC" {% if writer.pro == 'SESAC' %}selected{% endif %}>SESAC</option>
      </select>
    </div>
    <div class="field">
      <label class="label">Master Contract</label>
      <select class="inp" name="has_master_contract">
        <option value="0" {% if not writer.has_master_contract %}selected{% endif %}>No</option>
        <option value="1" {% if writer.has_master_contract %}selected{% endif %}>Yes</option>
      </select>
    </div>
  </div>

  <div class="g g2" style="margin-bottom:12px">
    <div class="field">
      <label class="label">Default Publisher</label>
      <input class="inp" name="default_publisher" value="{{ writer.default_publisher or default_publisher_for_pro(writer.pro) }}">
    </div>
    <div class="field">
      <label class="label">Default Publisher IPI</label>
      <input class="inp" name="default_publisher_ipi" value="{{ writer.default_publisher_ipi or default_publisher_ipi_for_pro(writer.pro) }}">
    </div>
  </div>

  <div class="g g4a" style="margin-bottom:12px">
    <div class="field">
      <label class="label">Street</label>
      <input class="inp" name="address" value="{{ writer.address or '' }}">
    </div>
    <div class="field">
      <label class="label">City</label>
      <input class="inp" name="city" value="{{ writer.city or '' }}">
    </div>
    <div class="field">
      <label class="label">State</label>
      <input class="inp" name="state" value="{{ writer.state or '' }}">
    </div>
    <div class="field">
      <label class="label">Zip</label>
      <input class="inp" name="zip_code" value="{{ writer.zip_code or '' }}">
    </div>
  </div>

  <div id="writerModalError" style="color:#ff8a8a;font-size:12px;margin-bottom:12px;"></div>

  <div style="display:flex;justify-content:flex-end;gap:8px">
    <button type="button" class="btn btn-sec" onclick="closeWriterModal()">Cancel</button>
    <button type="submit" class="btn btn-primary">Save Writer</button>
  </div>
</form>
"""

# ================================================================
# HELPERS
# ================================================================



# ================================================================
# PHASE 2 — RELEASES HTML TEMPLATES
# ================================================================

RELEASES_LIST_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Releases - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("releases_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
{% with messages = get_flashed_messages() %}{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128191;</div>
    <div><div class="ph-title">Releases</div><div class="ph-sub">Albums, EPs and Singles</div></div>
  </div>
  <div class="ph-actions">
    <a href="/releases/new" class="btn btn-primary" style="color:#fff">+ New Release</a>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#128269;</div><span class="card-title">Search</span></div>
  <div class="card-body">
    <form method="get" style="display:flex;gap:8px;flex-wrap:wrap">
      <input class="inp" name="q" value="{{ q }}" placeholder="Search by title, artist, track, UPC..." style="max-width:340px">
      <select class="inp" name="sort" style="max-width:220px">
        <option value="newest" {% if sort == "newest" %}selected{% endif %}>Newest First</option>
        <option value="oldest" {% if sort == "oldest" %}selected{% endif %}>Oldest First</option>
        <option value="title_asc" {% if sort == "title_asc" %}selected{% endif %}>Title A-Z</option>
        <option value="title_desc" {% if sort == "title_desc" %}selected{% endif %}>Title Z-A</option>
      </select>
      <button class="btn btn-sec" type="submit">Apply</button>
      {% if q or sort != "newest" %}<a href="/releases" class="btn btn-sec">Clear</a>{% endif %}
    </form>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#128191;</div><span class="card-title">All Releases</span></div>
  <div class="tbl-wrap">
    <table class="tbl tbl-releases" style="table-layout:auto">
      <thead>
        <tr>
          <th style="width:28%">Title</th>
          <th>Type</th>
          <th>Artist(s)</th>
          <th>Tracks</th>
          <th style="white-space:nowrap">Release Date</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody>
        {% for r in releases %}
        <tr class="rel-row" data-rel="{{ r.id }}" onclick="toggleRel({{ r.id }})">
          <td>
            <span class="expand-chevron">&#9658;</span>
            <span style="font-weight:600">{{ r.title }}</span>
            {% if r.release_date %}<div style="font-size:11px;color:var(--t3);margin-top:2px;margin-left:16px">{{ r.release_date.strftime('%b %d, %Y') }}</div>{% endif %}
          </td>
          <td><span class="tag tag-full">{{ r.release_type }}</span></td>
          <td style="color:var(--t2);font-size:12px">{{ r.artist_display }}</td>
          <td style="color:var(--t2)">{{ r.tracks|length }}</td>
          <td style="white-space:nowrap;font-size:12px;color:var(--t2)">{{ r.release_date.strftime('%b %d, %Y') if r.release_date else '--' }}</td>
          <td><span class="status s-{{ r.status }}"><span class="status-dot"></span>{{ r.status | title }}</span></td>
        </tr>
        <tr class="rel-detail-row" id="rel-detail-{{ r.id }}">
          <td colspan="6">
            <div class="work-detail-inner">
              <div class="wd-section">
                <div class="wd-label">Tracks</div>
                {% if r.tracks %}
                <table class="wd-writers-tbl">
                  <thead><tr><th>#</th><th>Title</th><th>Duration</th><th>ISRC</th><th>Linked Works</th></tr></thead>
                  <tbody>
                    {% for t in r.tracks %}
                    <tr>
                      <td style="color:var(--t3)">{{ t.track_number or '—' }}</td>
                      <td style="font-weight:600">{{ t.primary_title }}</td>
                      <td style="font-family:var(--fm)">{{ t.duration or '--' }}</td>
                      <td style="font-family:var(--fm)">{{ t.isrc or '--' }}</td>
                      <td>{% for tw in t.track_works %}<span class="tag tag-full" style="font-size:11px">{{ tw.work.title }}</span> {% endfor %}</td>
                    </tr>
                    {% endfor %}
                  </tbody>
                </table>
                {% else %}
                <span style="font-size:12px;color:var(--t3)">No tracks yet.</span>
                {% endif %}
              </div>
              <div class="wd-section">
                <div class="wd-label">Release Info</div>
                <table class="wd-writers-tbl">
                  <tbody>
                    <tr><td style="color:var(--t3)">Distributor</td><td>{{ r.distributor or '--' }}</td></tr>
                    <tr><td style="color:var(--t3)">UPC</td><td style="font-family:var(--fm)">{{ r.upc or '--' }}</td></tr>
                    <tr><td style="color:var(--t3)">Total Tracks</td><td>{{ r.num_tracks or r.tracks|length }}</td></tr>
                  </tbody>
                </table>
                <div style="display:flex;gap:8px;margin-top:16px">
                  <a href="/releases/{{ r.id }}" class="btn btn-primary btn-sm" style="color:#fff" onclick="event.stopPropagation()">Full View</a>
                  <a href="/releases/{{ r.id }}/edit" class="btn btn-sec btn-sm" onclick="event.stopPropagation()">Edit</a>
                </div>
              </div>
            </div>
          </td>
        </tr>
        {% endfor %}
        {% if not releases %}<tr class="empty"><td colspan="6">No releases found{% if q %} for "{{ q }}"{% endif %}.</td></tr>{% endif %}
      </tbody>
    </table>
  </div>
  {% if pagination.pages > 1 %}
  <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-top:1px solid var(--b1);font-size:13px;color:var(--t2)">
    <span>{{ pagination.total }} releases &mdash; page {{ pagination.page }} of {{ pagination.pages }}</span>
    <div style="display:flex;gap:6px">
      {% if pagination.has_prev %}<a href="?q={{ q }}&sort={{ sort }}&page={{ pagination.prev_num }}" class="btn btn-sec btn-sm">&#8592; Prev</a>{% endif %}
      {% if pagination.has_next %}<a href="?q={{ q }}&sort={{ sort }}&page={{ pagination.next_num }}" class="btn btn-sec btn-sm">Next &#8594;</a>{% endif %}
    </div>
  </div>
  {% endif %}
</div>
</div>
</main>
</div>
""" + _SB_JS + """
<style>
.rel-row{cursor:pointer;transition:background .15s}
.rel-detail-row{display:none}
.rel-detail-row.open{display:table-row}
@media(max-width:768px){
  .tbl-releases th:nth-child(3),.tbl-releases td:nth-child(3),
  .tbl-releases th:nth-child(5),.tbl-releases td:nth-child(5){display:none}
}
</style>
<script>
function toggleRel(id) {
  var row = document.getElementById('rel-detail-' + id);
  var hdr = document.querySelector('[data-rel="' + id + '"]');
  var isOpen = row.classList.contains('open');
  document.querySelectorAll('.rel-detail-row.open').forEach(function(r){ r.classList.remove('open'); });
  document.querySelectorAll('.rel-row.open').forEach(function(r){ r.classList.remove('open'); });
  if (!isOpen) { row.classList.add('open'); hdr.classList.add('open'); }
}
</script>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""


RELEASE_FORM_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ 'Edit' if release else 'New' }} Release - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("releases_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
{% with messages = get_flashed_messages() %}{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128191;</div>
    <div>
      <div class="ph-title">{{ 'Edit Release' if release else 'New Release' }}</div>
      <div class="ph-sub">{{ release.title if release else 'Add album, EP or single details and link tracks to compositions' }}</div>
    </div>
  </div>
</div>

<form method="post" id="releaseForm">

<!-- RELEASE INFO -->
<div class="card" style="overflow:visible">
  <div class="card-hd"><div class="card-ico">&#8505;</div><span class="card-title">Release Information</span></div>
  <div class="card-body" style="overflow:visible">
    <div class="wc-sec">General</div>
    <div class="g g2">
      <div class="field">
        <label class="label">Release Title *</label>
        <input class="inp" name="title" required value="{{ release.title if release else '' }}" placeholder="Album / EP / Single title">
      </div>
      <div class="field">
        <label class="label">Release Type *</label>
        <select class="inp" name="release_type" required>
          <option value="">Select type...</option>
          {% for rt in ['Album','EP','Single'] %}
          <option value="{{ rt }}" {{ 'selected' if release and release.release_type == rt }}>{{ rt }}</option>
          {% endfor %}
        </select>
      </div>
    </div>
    <div class="g g4" style="margin-top:12px">
      <div class="field">
        <label class="label">UPC <span style="color:var(--t3);font-size:11px">assign later</span></label>
        <input class="inp" name="upc" value="{{ release.upc if release else '' }}" placeholder="Leave blank">
      </div>
      <div class="field">
        <label class="label">Release Date</label>
        <input class="inp" type="date" name="release_date" value="{{ release.release_date.strftime('%Y-%m-%d') if release and release.release_date else '' }}">
      </div>
      <div class="field">
        <label class="label">Number of Tracks</label>
        <input class="inp" name="num_tracks" type="number" min="1" value="{{ release.num_tracks if release and release.num_tracks else '' }}" placeholder="e.g. 12">
      </div>
      <div class="field">
        <label class="label">Distributor</label>
        <input class="inp" name="distributor" value="{{ release.distributor if release and release.distributor else 'Believe' }}" placeholder="Believe">
      </div>
      <div class="field">
        <label class="label">Status</label>
        <select class="inp" name="status">
          {% for s in ['draft','ready','delivered'] %}
          <option value="{{ s }}" {{ 'selected' if release and release.status == s else ('selected' if not release and s == 'draft' else '') }}>{{ s | title }}</option>
          {% endfor %}
        </select>
      </div>
    </div>
    <div style="display:flex;align-items:center;justify-content:space-between;margin-top:18px">
      <div class="wc-sec" style="margin:0">Album Artists</div>
      <button type="button" class="btn btn-sec btn-xs" onclick="addArtist('albumArtistList')">+ Add Artist</button>
    </div>
    <div id="albumArtistList" style="margin-top:8px">
      {% if artists %}
        {% for a in artists %}
        <div class="artist-row" style="display:flex;gap:8px;margin-bottom:6px;align-items:center">
          <input class="inp" name="artist_1" value="{{ a }}" placeholder="{% if loop.first %}Primary artist *{% else %}Additional artist{% endif %}" style="flex:1">
          {% if not loop.first %}<button type="button" class="btn btn-xs" style="color:var(--ar);border-color:var(--ar);background:transparent" onclick="this.closest('.artist-row').remove()">X</button>{% endif %}
        </div>
        {% endfor %}
      {% else %}
        <div class="artist-row" style="display:flex;gap:8px;margin-bottom:6px;align-items:center">
          <input class="inp" name="artist_1" placeholder="Primary artist *" style="flex:1">
        </div>
      {% endif %}
    </div>
  </div>
</div>

<!-- TRACKS -->
<div class="card">
  <div class="card-hd">
    <div class="card-ico">&#127925;</div>
    <span class="card-title">Tracks</span>
    <div class="card-actions">
      <button type="button" class="btn btn-sec btn-sm" onclick="addTrack()">+ Add Track</button>
    </div>
  </div>
  <div class="card-body" id="tracksContainer">
    {% if tracks %}
    {% for t in tracks %}
    <div class="track-block" data-track-id="{{ t.id }}">
      <input type="hidden" name="track_id[]" value="{{ t.id }}">
      <div class="wc-hd" style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;padding-bottom:10px;border-bottom:1px solid var(--b0)">
        <div class="track-label" style="font-weight:700;font-size:14px;color:var(--a)">Track {{ loop.index }}{% if t.primary_title %} <span style="color:#fff;font-weight:400">— {{ t.primary_title }}</span>{% endif %}</div>
        <button type="button" class="btn btn-xs" style="color:var(--ar);border-color:var(--ar);background:transparent" onclick="removeTrack(this)">Remove</button>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
        <div class="wc-sec" style="margin:0">Track Artists</div>
        <button type="button" class="btn btn-sec btn-xs" onclick="addArtist('tartist-{{ t.id }}')">+ Add Artist</button>
      </div>
      <div id="tartist-{{ t.id }}" style="margin-top:8px">
        {% set tartists = t.artists_list if t.artists_list else artists %}
        {% if tartists %}
          {% for a in tartists %}
          <div class="artist-row" style="display:flex;gap:8px;margin-bottom:6px;align-items:center">
            <input class="inp" name="track_artist_{{ t.id }}[]" value="{{ a }}" placeholder="{% if loop.first %}Primary artist{% else %}Additional artist{% endif %}" style="flex:1">
            {% if not loop.first %}<button type="button" class="btn btn-xs" style="color:var(--ar);border-color:var(--ar);background:transparent" onclick="this.closest('.artist-row').remove()">X</button>{% endif %}
          </div>
          {% endfor %}
        {% else %}
          <div class="artist-row" style="display:flex;gap:8px;margin-bottom:6px;align-items:center">
            <input class="inp" name="track_artist_{{ t.id }}[]" placeholder="Primary artist" style="flex:1">
          </div>
        {% endif %}
      </div>
      <div class="g g4" style="margin-top:10px">
        <div class="field"><label class="label">Primary Title *</label><input class="inp" name="primary_title[]" required value="{{ t.primary_title }}" placeholder="Track title" oninput="updateTrackLabel(this)"></div>
        <div class="field"><label class="label">Duration</label><input class="inp" name="duration[]" value="{{ t.duration or '' }}" placeholder="3:45"></div>
        <div class="field"><label class="label">ISRC <span style="color:var(--t3);font-size:11px">assign later</span></label><input class="inp" name="isrc[]" value="{{ t.isrc or '' }}" placeholder="Leave blank"></div>
        <div class="field"><label class="label">Track #</label><input class="inp" name="track_number[]" type="number" value="{{ t.track_number or '' }}" placeholder="#"></div>
      </div>
      {% set has_alt_titles = t.recording_title or t.aka_title or t.aka_type_code %}
      <div style="margin-top:10px">
        <button type="button" class="other-titles-toggle" onclick="toggleOtherTitles(this)" style="background:none;border:none;cursor:pointer;color:var(--t2);font-size:12px;padding:0;display:flex;align-items:center;gap:5px">
          <span class="ot-arrow" style="display:inline-block;transition:transform .2s;{{ 'transform:rotate(90deg)' if has_alt_titles else '' }}">&#9654;&#65038;</span>
          <span>Other Titles</span>
        </button>
        <div class="other-titles-body g g3" style="margin-top:8px;{{ '' if has_alt_titles else 'display:none' }}">
          <div class="field"><label class="label">Recording Title</label><input class="inp" name="recording_title[]" value="{{ t.recording_title or '' }}" placeholder="If different from primary"></div>
          <div class="field"><label class="label">AKA Title</label><input class="inp" name="aka_title[]" value="{{ t.aka_title or '' }}" placeholder="Alternate title"></div>
          <div class="field"><label class="label">AKA Type Code</label><input class="inp" name="aka_type_code[]" value="{{ t.aka_type_code or '' }}" placeholder="AT, TT..."></div>
        </div>
      </div>
      <div class="g g4" style="margin-top:10px">
        <div class="field"><label class="label">Genre</label><input class="inp" name="genre[]" value="{{ t.genre or '' }}" placeholder="Genre"></div>
        <div class="field"><label class="label">Recording Date</label><input class="inp" type="date" name="recording_date[]" value="{{ t.recording_date.strftime('%Y-%m-%d') if t.recording_date else '' }}"></div>
        <div class="field"><label class="label">Recording Engineer</label><input class="inp" name="recording_engineer[]" value="{{ t.recording_engineer or '' }}" placeholder="Engineer name"></div>
        <div class="field"><label class="label">Producer</label><input class="inp" name="producer[]" value="{{ t.producer or '' }}" placeholder="Producer name"></div>
      </div>
      <div class="g g4" style="margin-top:10px">
        <div class="field"><label class="label">Executive Producer</label><input class="inp" name="executive_producer[]" value="{{ t.executive_producer or '' }}" placeholder="Exec producer"></div>
      </div>
      <div class="g g2" style="margin-top:10px">
        <div class="field"><label class="label">Track Label</label><input class="inp" name="track_label[]" value="{{ t.track_label or '' }}" placeholder="Label name"></div>
        <div class="field"><label class="label">Track P Line</label><input class="inp" name="track_p_line[]" value="{{ t.track_p_line or '' }}" placeholder="(P) 2024 Label Name"></div>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:14px">
        <div class="wc-sec" style="color:var(--a);margin:0">Linked Works (Compositions)</div>
        <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--t2);cursor:pointer">
          <input type="checkbox" name="is_cover[]" value="1" {% if t.is_cover %}checked{% endif %} onchange="toggleCoverMode(this,'{{ t.id }}')"> Cover
        </label>
      </div>
      <div class="linked-works" id="linked-works-{{ t.id }}" {% if t.is_cover %}style="display:none"{% endif %}>
        {% for tw in t.track_works %}
        <div class="linked-work-row" style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
          <input type="hidden" name="linked_work_ids_{{ t.id }}[]" value="{{ tw.work_id }}">
          <span class="tag tag-s1" style="flex:1">{{ tw.work.title }} — {{ tw.work.work_writers|map(attribute='writer')|map(attribute='full_name')|join(', ') }}</span>
          <input class="inp" style="width:140px;font-size:12px" name="linked_work_notes_{{ t.id }}[]" value="{{ tw.notes or '' }}" placeholder="Notes (optional)">
          <button type="button" class="btn btn-xs" style="color:var(--ar);border-color:var(--ar);background:transparent" onclick="this.closest('.linked-work-row').remove()">X</button>
        </div>
        {% endfor %}
      </div>
      <div id="work-search-area-{{ t.id }}" {% if t.is_cover %}style="display:none"{% endif %}>
        <div class="inp-wrap" style="margin-top:8px">
          <span class="inp-ico">&#128395;</span>
          <input class="inp work-search-inp" placeholder="Search works to link..." data-track-id="{{ t.id }}" oninput="searchWorks(this)">
        </div>
        <div class="work-suggestions" id="work-sugg-{{ t.id }}" style="display:none;background:var(--bg4);border:1px solid var(--b0);border-radius:var(--rs);overflow:hidden;margin-top:4px"></div>
      </div>
      <div id="cover-writers-{{ t.id }}" style="{% if not t.is_cover %}display:none;{% endif %}margin-top:8px">
        <div class="wc-sec" style="margin-bottom:6px;font-size:12px;color:var(--t2)">Original Writers (Cover)</div>
        <input class="inp" name="cover_writers[]" placeholder="e.g. John Lennon, Paul McCartney" value="{{ t.cover_writers or '' }}">
      </div>
    </div>
    <hr style="border:none;border-top:1px solid var(--b0);margin:20px 0">
    {% endfor %}
    {% else %}
    <div id="noTracksMsg" style="color:var(--t3);font-size:13px;text-align:center;padding:24px">No tracks yet. Click <strong>+ Add Track</strong> above to get started.</div>
    {% endif %}
  </div>
</div>

<div class="action-bar">
  <button type="button" class="btn btn-sec" onclick="addTrack()">+ Add Track</button>
  <div class="ab-space"></div>
  {% if release %}
  <form method="post" action="/releases/{{ release.id }}/delete" style="display:inline" onsubmit="return confirm('Delete this release?')">
    <button type="submit" class="btn btn-sec" style="color:var(--ar);border-color:var(--ar)">Delete</button>
  </form>
  {% endif %}
  <a href="/releases" class="btn btn-sec">Cancel</a>
  <button type="submit" class="btn btn-primary" style="color:#fff">Save Release</button>
</div>
</form>
</div>
</main>
</div>
""" + _SB_JS + """
<script>
var trackCount = {{ tracks|length if tracks else 0 }};

function addArtist(containerId) {
  var container = document.getElementById(containerId);
  if (!container) return;
  var existing = container.querySelectorAll('.artist-row input');
  var name = existing.length > 0 ? existing[0].name : 'artist_1';
  var row = document.createElement('div');
  row.className = 'artist-row';
  row.style.cssText = 'display:flex;gap:8px;margin-bottom:6px;align-items:center';
  var inp = document.createElement('input'); inp.className = 'inp';
  inp.name = name; inp.placeholder = 'Additional artist'; inp.style.flex = '1';
  var btn = document.createElement('button'); btn.type = 'button'; btn.className = 'btn btn-xs';
  btn.style.cssText = 'color:var(--ar);border-color:var(--ar);background:transparent';
  btn.textContent = 'X';
  btn.onclick = function(){ this.closest('.artist-row').remove(); };
  row.appendChild(inp); row.appendChild(btn);
  container.appendChild(row);
}

function makeField(labelText, inputAttrs) {
  var d = document.createElement('div'); d.className = 'field';
  var l = document.createElement('label'); l.className = 'label'; l.textContent = labelText;
  var inp = document.createElement('input'); inp.className = 'inp';
  for (var k in inputAttrs) inp[k] = inputAttrs[k];
  d.appendChild(l); d.appendChild(inp);
  return d;
}
function makeSection(text) {
  var s = document.createElement('div'); s.className = 'wc-sec'; s.textContent = text; return s;
}
function makeGrid(fields) {
  var g = document.createElement('div'); g.className = 'g g4';
  fields.forEach(function(f){ g.appendChild(f); });
  return g;
}

function addTrack() {
  trackCount++;
  var idx = trackCount;
  var noMsg = document.getElementById('noTracksMsg');
  if (noMsg) noMsg.remove();
  var container = document.getElementById('tracksContainer');

  var block = document.createElement('div');
  block.className = 'track-block';

  // hidden id
  var hiddenId = document.createElement('input');
  hiddenId.type = 'hidden'; hiddenId.name = 'track_id[]'; hiddenId.value = '';
  block.appendChild(hiddenId);

  // header
  var hdr = document.createElement('div');
  hdr.style.cssText = 'display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;padding-bottom:10px;border-bottom:1px solid var(--b0)';
  var title = document.createElement('div');
  title.className = 'track-label';
  title.style.cssText = 'font-weight:700;font-size:14px;color:var(--a)';
  title.textContent = 'Track ' + idx;
  var rmBtn = document.createElement('button');
  rmBtn.type = 'button'; rmBtn.className = 'btn btn-xs';
  rmBtn.style.cssText = 'color:var(--ar);border-color:var(--ar);background:transparent';
  rmBtn.textContent = 'Remove';
  rmBtn.onclick = function(){ removeTrack(this); };
  hdr.appendChild(title); hdr.appendChild(rmBtn);
  block.appendChild(hdr);

  // Track Artists (first)
  var asHdr = document.createElement('div');
  asHdr.style.cssText = 'display:flex;align-items:center;justify-content:space-between;margin-top:4px';
  var asLbl = makeSection('Track Artists'); asLbl.style.margin = '0';
  var asBtn = document.createElement('button'); asBtn.type = 'button'; asBtn.className = 'btn btn-sec btn-xs';
  asBtn.textContent = '+ Add Artist';
  var artistListId = 'tartist-new-' + idx;
  asBtn.setAttribute('onclick', 'addArtist("' + artistListId + '")');
  asHdr.appendChild(asLbl); asHdr.appendChild(asBtn);
  block.appendChild(asHdr);
  var artistList = document.createElement('div'); artistList.id = artistListId; artistList.style.marginTop = '8px';
  var albumArtistInputs = document.querySelectorAll('#albumArtistList input[name="artist_1"]');
  var albumArtists = [];
  albumArtistInputs.forEach(function(inp){ if (inp.value.trim()) albumArtists.push(inp.value.trim()); });
  if (albumArtists.length === 0) albumArtists = [''];
  albumArtists.forEach(function(val, ai) {
    var row = document.createElement('div');
    row.className = 'artist-row';
    row.style.cssText = 'display:flex;gap:8px;margin-bottom:6px;align-items:center';
    var inp = document.createElement('input'); inp.className = 'inp';
    inp.name = 'track_artist_new_' + idx + '[]';
    inp.value = val;
    inp.placeholder = ai === 0 ? 'Primary artist' : 'Additional artist';
    inp.style.flex = '1';
    row.appendChild(inp);
    if (ai > 0) {
      var xb = document.createElement('button'); xb.type = 'button'; xb.className = 'btn btn-xs';
      xb.style.cssText = 'color:var(--ar);border-color:var(--ar);background:transparent';
      xb.textContent = 'X';
      xb.onclick = function(){ this.closest('.artist-row').remove(); };
      row.appendChild(xb);
    }
    artistList.appendChild(row);
  });
  block.appendChild(artistList);

  // Row 2: Primary Title, Duration, ISRC, Track #
  var r1 = makeGrid([
    makeField('Primary Title', {name:'primary_title[]', placeholder:'Track title', required:true}),
    makeField('Duration', {name:'duration[]', placeholder:'3:45'}),
    makeField('ISRC (assign later)', {name:'isrc[]', placeholder:'Leave blank'}),
    makeField('Track #', {name:'track_number[]', type:'number', placeholder:'#', value:idx})
  ]); r1.style.marginTop = '10px';
  r1.querySelector('input[name="primary_title[]"]').addEventListener('input', function(){ updateTrackLabel(this); });
  block.appendChild(r1);

  // Other Titles collapsible (below Primary Title)
  var otWrap = document.createElement('div'); otWrap.style.marginTop = '10px';
  var otBtn = document.createElement('button'); otBtn.type = 'button'; otBtn.className = 'other-titles-toggle';
  otBtn.style.cssText = 'background:none;border:none;cursor:pointer;color:var(--t2);font-size:12px;padding:0;display:flex;align-items:center;gap:5px';
  otBtn.setAttribute('onclick', 'toggleOtherTitles(this)');
  var otArrow = document.createElement('span'); otArrow.className = 'ot-arrow';
  otArrow.style.cssText = 'display:inline-block;transition:transform .2s';
  otArrow.innerHTML = '&#9654;&#65038;';
  var otLabel = document.createElement('span'); otLabel.textContent = 'Other Titles';
  otBtn.appendChild(otArrow); otBtn.appendChild(otLabel);
  var otBody = document.createElement('div'); otBody.className = 'other-titles-body g g3';
  otBody.style.cssText = 'margin-top:8px;display:none';
  otBody.appendChild(makeField('Recording Title', {name:'recording_title[]', placeholder:'If different from primary'}));
  otBody.appendChild(makeField('AKA Title', {name:'aka_title[]', placeholder:'Alternate title'}));
  otBody.appendChild(makeField('AKA Type Code', {name:'aka_type_code[]', placeholder:'AT, TT...'}));
  otWrap.appendChild(otBtn); otWrap.appendChild(otBody);
  block.appendChild(otWrap);

  // Row 3: Genre, Recording Date, Recording Engineer, Producer
  var r2 = makeGrid([
    makeField('Genre', {name:'genre[]', placeholder:'Genre'}),
    makeField('Recording Date', {name:'recording_date[]', type:'date'}),
    makeField('Recording Engineer', {name:'recording_engineer[]', placeholder:'Engineer name'}),
    makeField('Producer', {name:'producer[]', placeholder:'Producer name'})
  ]); r2.style.marginTop = '10px'; block.appendChild(r2);

  // Row 4: Executive Producer only
  var r3 = makeGrid([
    makeField('Executive Producer', {name:'executive_producer[]', placeholder:'Exec producer'})
  ]); r3.style.marginTop = '10px'; block.appendChild(r3);

  // Row 5: Track Label, Track P Line
  var r4 = document.createElement('div'); r4.className = 'g g2'; r4.style.marginTop = '10px';
  r4.appendChild(makeField('Track Label', {name:'track_label[]', placeholder:'Label name'}));
  r4.appendChild(makeField('Track P Line', {name:'track_p_line[]', placeholder:'(P) 2024 Label Name'}));
  block.appendChild(r4);

  // Linked Works section
  var lwHd = document.createElement('div');
  lwHd.style.cssText = 'display:flex;align-items:center;justify-content:space-between;margin-top:14px';
  var lwTitle = makeSection('Linked Works (Compositions)'); lwTitle.style.cssText = 'color:var(--a);margin:0';
  var coverLabel = document.createElement('label');
  coverLabel.style.cssText = 'display:flex;align-items:center;gap:6px;font-size:12px;color:var(--t2);cursor:pointer';
  var coverCb = document.createElement('input'); coverCb.type = 'checkbox'; coverCb.name = 'is_cover[]'; coverCb.value = '1';
  coverCb.setAttribute('onchange', 'toggleCoverMode(this,"new-' + idx + '")');
  coverLabel.appendChild(coverCb); coverLabel.appendChild(document.createTextNode(' Cover'));
  lwHd.appendChild(lwTitle); lwHd.appendChild(coverLabel);
  block.appendChild(lwHd);
  var linkedDiv = document.createElement('div');
  linkedDiv.className = 'linked-works'; linkedDiv.id = 'linked-works-new-' + idx;
  block.appendChild(linkedDiv);
  var searchArea = document.createElement('div');
  searchArea.id = 'work-search-area-new-' + idx;
  var searchWrap = document.createElement('div'); searchWrap.className = 'inp-wrap'; searchWrap.style.marginTop = '8px';
  var searchIco = document.createElement('span'); searchIco.className = 'inp-ico'; searchIco.textContent = '\u266B';
  var searchInp = document.createElement('input'); searchInp.className = 'inp work-search-inp';
  searchInp.placeholder = 'Search works to link...';
  searchInp.setAttribute('data-track-new', idx);
  searchInp.setAttribute('oninput', 'searchWorks(this)');
  searchWrap.appendChild(searchIco); searchWrap.appendChild(searchInp);
  searchArea.appendChild(searchWrap);
  var suggDiv = document.createElement('div');
  suggDiv.id = 'work-sugg-new-' + idx;
  suggDiv.style.cssText = 'display:none;background:var(--bg4);border:1px solid var(--b0);border-radius:var(--rs);overflow:hidden;margin-top:4px';
  searchArea.appendChild(suggDiv);
  block.appendChild(searchArea);
  var coverWritersDiv = document.createElement('div');
  coverWritersDiv.id = 'cover-writers-new-' + idx;
  coverWritersDiv.style.cssText = 'display:none;margin-top:8px';
  var cwLabel = document.createElement('div');
  cwLabel.className = 'wc-sec';
  cwLabel.style.cssText = 'margin-bottom:6px;font-size:12px;color:var(--t2)';
  cwLabel.textContent = 'Original Writers (Cover)';
  var cwInp = document.createElement('input');
  cwInp.className = 'inp'; cwInp.name = 'cover_writers[]';
  cwInp.placeholder = 'e.g. John Lennon, Paul McCartney';
  coverWritersDiv.appendChild(cwLabel);
  coverWritersDiv.appendChild(cwInp);
  block.appendChild(coverWritersDiv);

  var hr = document.createElement('hr');
  hr.style.cssText = 'border:none;border-top:1px solid var(--b0);margin:18px 0';
  container.appendChild(block);
  container.appendChild(hr);
}

function updateTrackLabel(inp) {
  var block = inp.closest('.track-block');
  if (!block) return;
  var label = block.querySelector('.track-label');
  if (!label) return;
  var num = label.textContent.split(' \u2014')[0].split(' -')[0].trim();
  if (inp.value.trim()) {
    label.innerHTML = num + ' <span style="color:#fff;font-weight:400">\u2014 ' + inp.value.trim() + '</span>';
  } else {
    label.textContent = num;
  }
}

function toggleOtherTitles(btn) {
  var body = btn.nextElementSibling;
  var arrow = btn.querySelector('.ot-arrow');
  var open = body.style.display !== 'none';
  body.style.display = open ? 'none' : 'grid';
  arrow.style.transform = open ? '' : 'rotate(90deg)';
}

function removeTrack(btn) {
  var block = btn.closest('.track-block');
  var hr = block.nextElementSibling;
  if (hr && hr.tagName === 'HR') hr.remove();
  block.remove();
}

function searchWorks(inp) {
  var q = inp.value.trim();
  var trackId = inp.dataset.trackId || ('new-' + inp.dataset.trackNew);
  var sugg = document.getElementById('work-sugg-' + trackId);
  if (!sugg) return;
  if (q.length < 2) { sugg.style.display = 'none'; return; }
  fetch('/works/search?q=' + encodeURIComponent(q))
    .then(function(r){ return r.json(); })
    .then(function(data){
      sugg.innerHTML = '';
      data.forEach(function(w){
        var item = document.createElement('div');
        item.className = 'sugg-item';
        item.style.cssText = 'padding:8px 12px;cursor:pointer;border-bottom:1px solid var(--b1);font-size:13px';
        item.setAttribute('data-work-id', w.id);
        item.setAttribute('data-work-title', w.title);
        item.setAttribute('data-work-writers', w.writers || '');
        item.setAttribute('data-track-id', trackId);
        item.innerHTML = '<span style="font-weight:600">' + w.title + '</span>'
          + '<span style="color:var(--t3);font-size:11px;margin-left:8px">' + (w.writers || '') + '</span>';
        item.addEventListener('click', function(){
          var wTitle = this.getAttribute('data-work-title');
          var wWriters = this.getAttribute('data-work-writers');
          var display = wTitle + (wWriters ? ' \u2014 ' + wWriters : '');
          linkWork(this, this.getAttribute('data-track-id'), this.getAttribute('data-work-id'), display);
        });
        sugg.appendChild(item);
      });
      // "Create New Work" option
      var createItem = document.createElement('div');
      createItem.className = 'sugg-item';
      createItem.style.cssText = 'padding:8px 12px;cursor:pointer;border-bottom:1px solid var(--b1);font-size:13px;color:var(--a);font-weight:600';
      createItem.innerHTML = '+ Create &ldquo;' + q.replace(/</g,'&lt;') + '&rdquo; as new work';
      createItem.addEventListener('click', function(){
        sugg.style.display = 'none';
        openQuickWorkModal(trackId, q);
      });
      sugg.appendChild(createItem);
      // "Cover" option
      var coverItem = document.createElement('div');
      coverItem.className = 'sugg-item';
      coverItem.style.cssText = 'padding:8px 12px;cursor:pointer;font-size:13px;color:var(--t2);font-weight:600';
      coverItem.innerHTML = 'Cover \u2014 no publishing required';
      coverItem.addEventListener('click', function(){
        sugg.style.display = 'none';
        var suggEl = document.getElementById('work-sugg-' + trackId);
        if (suggEl) {
          var block = suggEl.closest('.track-block');
          if (block) {
            var coverCb = block.querySelector('input[type=checkbox][name="is_cover[]"]');
            if (coverCb && !coverCb.checked) { coverCb.checked = true; toggleCoverMode(coverCb, trackId); }
          }
        }
        document.querySelectorAll('.work-search-inp').forEach(function(i){
          if (i.dataset.trackId == trackId || ('new-' + i.dataset.trackNew) == trackId) i.value = '';
        });
      });
      sugg.appendChild(coverItem);
      sugg.style.display = 'block';
    });
}

function toggleCoverMode(cb, trackId) {
  var linkedDiv = document.getElementById('linked-works-' + trackId);
  var searchArea = document.getElementById('work-search-area-' + trackId);
  var coverWriters = document.getElementById('cover-writers-' + trackId);
  if (cb.checked) {
    if (linkedDiv) linkedDiv.style.display = 'none';
    if (searchArea) searchArea.style.display = 'none';
    if (coverWriters) coverWriters.style.display = 'block';
  } else {
    if (linkedDiv) linkedDiv.style.display = '';
    if (searchArea) searchArea.style.display = '';
    if (coverWriters) coverWriters.style.display = 'none';
  }
}

// ── Quick Work Modal (iframe) ─────────────────────────────────────────────────
function openQuickWorkModal(trackId, prefillTitle) {
  var modal = document.getElementById('quickWorkModal');
  if (!modal) return;
  var frame = document.getElementById('qwm-iframe');
  var url = '/?modal=1' + (prefillTitle ? '&work_title=' + encodeURIComponent(prefillTitle) : '');
  frame.src = url;
  modal.dataset.trackId = trackId;
  modal.style.display = 'flex';
}

function closeQuickWorkModal() {
  var modal = document.getElementById('quickWorkModal');
  if (!modal) return;
  modal.style.display = 'none';
  document.getElementById('qwm-iframe').src = 'about:blank';
}

function qwmAddWriter() {
  try {
    document.getElementById('qwm-iframe').contentWindow.addWriter();
  } catch(e) { console.warn('qwmAddWriter:', e); }
}

function qwmSave() {
  try {
    var iwin = document.getElementById('qwm-iframe').contentWindow;
    var form = iwin.document.querySelector('form');
    if (form) form.requestSubmit ? form.requestSubmit() : form.submit();
  } catch(e) { console.warn('qwmSave:', e); }
}

// Called by the iframe after a work is saved successfully
function onWorkCreated(workId, workTitle, workWriters, batchUrl) {
  var modal = document.getElementById('quickWorkModal');
  var trackId = modal ? modal.dataset.trackId : null;
  closeQuickWorkModal();
  if (trackId) {
    linkWork(null, trackId, workId, workTitle + (workWriters ? ' \u2014 ' + workWriters : ''));
  }
  if (batchUrl) {
    document.getElementById('qwm-session-link').href = batchUrl;
    document.getElementById('qwm-session-toast').style.display = 'flex';
    setTimeout(function(){ document.getElementById('qwm-session-toast').style.display = 'none'; }, 10000);
  }
}

document.addEventListener('DOMContentLoaded', function(){
  var modal = document.getElementById('quickWorkModal');
  if (!modal) return;
  modal.addEventListener('click', function(e){ if (e.target === modal) closeQuickWorkModal(); });
  var toastClose = document.getElementById('qwm-toast-close');
  if (toastClose) toastClose.addEventListener('click', function(){
    document.getElementById('qwm-session-toast').style.display = 'none';
  });
});

function linkWork(el, trackId, workId, workTitle) {
  var sugg = document.getElementById('work-sugg-' + trackId);
  sugg.style.display = 'none';
  var container = document.getElementById('linked-works-' + trackId);
  if (!container) return;
  var existing = container.querySelectorAll('input[type=hidden]');
  for (var i=0; i<existing.length; i++) { if (existing[i].value == workId) return; }
  var isNew = trackId.toString().indexOf('new') === 0;
  var cleanId = isNew ? trackId.replace('new-','') : trackId;
  var nameKey = isNew ? ('linked_work_ids_new_' + cleanId + '[]') : ('linked_work_ids_' + cleanId + '[]');
  var notesKey = isNew ? ('linked_work_notes_new_' + cleanId + '[]') : ('linked_work_notes_' + cleanId + '[]');
  var row = document.createElement('div');
  row.className = 'linked-work-row';
  row.style.cssText = 'display:flex;align-items:center;gap:8px;margin-bottom:6px';
  var inp1 = document.createElement('input');
  inp1.type = 'hidden'; inp1.name = nameKey; inp1.value = workId;
  var lbl = document.createElement('span');
  lbl.className = 'tag tag-s1'; lbl.style.flex = '1'; lbl.textContent = workTitle;
  var inp2 = document.createElement('input');
  inp2.className = 'inp'; inp2.style.cssText = 'width:140px;font-size:12px';
  inp2.name = notesKey; inp2.placeholder = 'Notes (optional)';
  var btn = document.createElement('button');
  btn.type = 'button'; btn.className = 'btn btn-xs';
  btn.style.cssText = 'color:var(--ar);border-color:var(--ar);background:transparent';
  btn.textContent = 'X';
  btn.onclick = function(){ this.closest('.linked-work-row').remove(); };
  row.appendChild(inp1); row.appendChild(lbl); row.appendChild(inp2); row.appendChild(btn);
  container.appendChild(row);
  document.querySelectorAll('.work-search-inp').forEach(function(inp){
    if (inp.dataset.trackId == trackId || inp.dataset.trackNew == cleanId) inp.value = '';
  });
}

document.addEventListener('click', function(e){
  document.querySelectorAll('.work-suggestions,.artist-suggestions,.title-suggestions').forEach(function(s){
    if (!s.contains(e.target)) s.style.display = 'none';
  });
});

// ── Artist autocomplete ──────────────────────────────────────────────────────
var _artistSuggStyle = 'position:absolute;top:100%;left:0;right:0;background:var(--bg4);border:1px solid var(--b0);border-radius:var(--rs);z-index:200;overflow:hidden;margin-top:2px';

function _ensureArtistSugg(inp) {
  var wrap = inp.parentElement;
  if (wrap.style.position !== 'relative') { wrap.style.position = 'relative'; }
  var id = 'asugg-' + inp.dataset.asuggId;
  var el = document.getElementById(id);
  if (!el) {
    el = document.createElement('div');
    el.id = id; el.className = 'artist-suggestions';
    el.style.cssText = _artistSuggStyle;
    el.style.display = 'none';
    wrap.appendChild(el);
  }
  return el;
}

var _artistSuggCounter = 0;
function setupArtistInput(inp) {
  if (inp.dataset.asuggId) return; // already wired
  inp.dataset.asuggId = ++_artistSuggCounter;
  inp.addEventListener('input', function() {
    var q = inp.value.trim();
    var sugg = _ensureArtistSugg(inp);
    if (q.length < 2) { sugg.style.display = 'none'; return; }
    fetch('/artists/search?q=' + encodeURIComponent(q))
      .then(function(r){ return r.json(); })
      .then(function(names){
        if (!names.length) { sugg.style.display = 'none'; return; }
        sugg.innerHTML = '';
        names.forEach(function(name){
          var item = document.createElement('div');
          item.style.cssText = 'padding:7px 12px;cursor:pointer;font-size:13px;border-bottom:1px solid var(--b1)';
          item.textContent = name;
          item.addEventListener('mousedown', function(e){
            e.preventDefault();
            inp.value = name;
            sugg.style.display = 'none';
          });
          sugg.appendChild(item);
        });
        sugg.style.display = 'block';
      });
  });
  inp.addEventListener('blur', function(){
    setTimeout(function(){ var s = _ensureArtistSugg(inp); s.style.display = 'none'; }, 150);
  });
}

// Wire existing artist inputs and observe new ones
document.addEventListener('DOMContentLoaded', function(){
  document.querySelectorAll('.artist-row input.inp').forEach(setupArtistInput);
});
document.addEventListener('focusin', function(e){
  if (e.target.matches('.artist-row input.inp')) setupArtistInput(e.target);
});

// ── Track title autocomplete ─────────────────────────────────────────────────
var _titleSuggStyle = 'position:absolute;top:100%;left:0;right:0;background:var(--bg4);border:1px solid var(--b0);border-radius:var(--rs);z-index:200;overflow:hidden;margin-top:2px';

function _ensureTitleSugg(inp) {
  var wrap = inp.parentElement;
  if (wrap.style.position !== 'relative') { wrap.style.position = 'relative'; }
  var el = wrap.querySelector('.title-suggestions');
  if (!el) {
    el = document.createElement('div');
    el.className = 'title-suggestions';
    el.style.cssText = _titleSuggStyle;
    el.style.display = 'none';
    wrap.appendChild(el);
  }
  return el;
}

function _fillTrackFromResult(inp, t) {
  var block = inp.closest('.track-block');
  if (!block) return;
  function set(selector, val) {
    var el = block.querySelector(selector);
    if (el && val) { el.value = val; el.dispatchEvent(new Event('input')); }
  }
  // primary title already filled via inp.value
  set('input[name="duration[]"]',           t.duration);
  set('input[name="isrc[]"]',               t.isrc);
  set('input[name="recording_title[]"]',    t.recording_title);
  set('input[name="aka_title[]"]',          t.aka_title);
  set('input[name="aka_type_code[]"]',      t.aka_type_code);
  set('input[name="genre[]"]',              t.genre);
  set('input[name="producer[]"]',           t.producer);
  set('input[name="recording_engineer[]"]', t.recording_engineer);
  set('input[name="executive_producer[]"]', t.executive_producer);
  set('input[name="track_label[]"]',        t.track_label);
  set('input[name="track_p_line[]"]',       t.track_p_line);
  // expand Other Titles if any alt title was filled
  if (t.recording_title || t.aka_title || t.aka_type_code) {
    var otBody = block.querySelector('.other-titles-body');
    var otArrow = block.querySelector('.ot-arrow');
    if (otBody) otBody.style.display = 'grid';
    if (otArrow) otArrow.style.transform = 'rotate(90deg)';
  }
  // fill artists from track
  if (t.artists && t.artists.length) {
    var artistContainerId = null;
    var hiddenId = block.querySelector('input[name="track_id[]"]');
    var tid = hiddenId ? hiddenId.value : '';
    if (tid) {
      artistContainerId = 'tartist-' + tid;
    } else {
      // new track — find the artistList div by class pattern
      var artistDiv = block.querySelector('[id^="tartist-new-"]');
      if (artistDiv) artistContainerId = artistDiv.id;
    }
    if (artistContainerId) {
      var ac = document.getElementById(artistContainerId);
      if (ac) {
        var artistInputName = ac.querySelector('input') ? ac.querySelector('input').name : null;
        ac.innerHTML = '';
        t.artists.forEach(function(name, ai) {
          var row = document.createElement('div');
          row.className = 'artist-row';
          row.style.cssText = 'display:flex;gap:8px;margin-bottom:6px;align-items:center';
          var ainp = document.createElement('input'); ainp.className = 'inp';
          ainp.name = artistInputName || (tid ? ('track_artist_' + tid + '[]') : ('track_artist_new_1[]'));
          ainp.value = name; ainp.style.flex = '1';
          ainp.placeholder = ai === 0 ? 'Primary artist' : 'Additional artist';
          setupArtistInput(ainp);
          row.appendChild(ainp);
          if (ai > 0) {
            var xb = document.createElement('button'); xb.type = 'button'; xb.className = 'btn btn-xs';
            xb.style.cssText = 'color:var(--ar);border-color:var(--ar);background:transparent';
            xb.textContent = 'X'; xb.onclick = function(){ this.closest('.artist-row').remove(); };
            row.appendChild(xb);
          }
          ac.appendChild(row);
        });
      }
    }
  }
}

function setupTitleInput(inp) {
  if (inp.dataset.titleWired) return;
  inp.dataset.titleWired = '1';
  inp.addEventListener('input', function() {
    var q = inp.value.trim();
    var sugg = _ensureTitleSugg(inp);
    if (q.length < 2) { sugg.style.display = 'none'; return; }
    fetch('/tracks/search?q=' + encodeURIComponent(q))
      .then(function(r){ return r.json(); })
      .then(function(tracks){
        if (!tracks.length) { sugg.style.display = 'none'; return; }
        sugg.innerHTML = '';
        tracks.forEach(function(t){
          var item = document.createElement('div');
          item.style.cssText = 'padding:7px 12px;cursor:pointer;font-size:13px;border-bottom:1px solid var(--b1)';
          item.innerHTML = '<span style="font-weight:600">' + t.primary_title + '</span>'
            + (t.artists && t.artists.length ? '<span style="color:var(--t3);font-size:11px;margin-left:8px">' + t.artists.slice(0,2).join(', ') + '</span>' : '');
          item.addEventListener('mousedown', function(e){
            e.preventDefault();
            inp.value = t.primary_title;
            inp.dispatchEvent(new Event('input'));
            sugg.style.display = 'none';
            _fillTrackFromResult(inp, t);
          });
          sugg.appendChild(item);
        });
        sugg.style.display = 'block';
      });
  });
  inp.addEventListener('blur', function(){
    setTimeout(function(){ var s = _ensureTitleSugg(inp); s.style.display = 'none'; }, 150);
  });
}

document.addEventListener('DOMContentLoaded', function(){
  document.querySelectorAll('input[name="primary_title[]"]').forEach(setupTitleInput);
});
document.addEventListener('focusin', function(e){
  if (e.target.matches('input[name="primary_title[]"]')) setupTitleInput(e.target);
});
</script>

<!-- Quick Work Modal -->
<div id="quickWorkModal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:10000;align-items:center;justify-content:center">
  <div style="background:var(--bg1);border:1px solid var(--b0);border-radius:14px;width:92%;max-width:960px;height:88vh;display:flex;flex-direction:column;overflow:hidden;position:relative">
    <div style="display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid var(--b0);background:var(--bg2);flex-shrink:0">
      <span style="font-weight:700;font-size:15px;color:var(--t1)">New Work</span>
      <button type="button" onclick="closeQuickWorkModal()" style="background:transparent;border:none;color:var(--t2);font-size:22px;cursor:pointer;line-height:1">&times;</button>
    </div>
    <iframe id="qwm-iframe" src="about:blank" style="flex:1;border:none;width:100%;min-height:0"></iframe>
    <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 18px;border-top:1px solid var(--b0);background:var(--bg2);flex-shrink:0;gap:10px">
      <button type="button" class="btn btn-sec" onclick="qwmAddWriter()">+ Add Writer</button>
      <div style="display:flex;gap:10px">
        <button type="button" class="btn btn-sec" onclick="closeQuickWorkModal()">Cancel</button>
        <button type="button" class="btn btn-primary" style="color:#fff" onclick="qwmSave()">Save Work to Session</button>
      </div>
    </div>
  </div>
</div>

<!-- Session toast (shown after quick-creating a work with a session) -->
<div id="qwm-session-toast" style="display:none;position:fixed;bottom:24px;right:24px;background:var(--bg2);border:1px solid var(--b0);border-radius:10px;padding:14px 18px;z-index:1100;align-items:center;gap:14px;box-shadow:0 4px 20px rgba(0,0,0,0.3)">
  <span style="font-size:13px;color:var(--t1)">Work saved to session.</span>
  <a id="qwm-session-link" href="#" target="_blank" style="font-size:13px;color:var(--a);font-weight:600">Open session &#8599;</a>
  <button type="button" id="qwm-toast-close" style="background:transparent;border:none;color:var(--t3);cursor:pointer;font-size:16px;line-height:1">&times;</button>
</div>

<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""


RELEASE_DETAIL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ release.title }} - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("releases_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128191;</div>
    <div>
      <div class="ph-title">{{ release.title }}</div>
      <div class="ph-sub">{{ release.release_type }} &mdash; {{ release.artist_display }}</div>
    </div>
  </div>
  <div class="ph-actions">
    <a href="/releases" class="btn btn-sec btn-sm">Back</a>
    <a href="/releases/{{ release.id }}/edit" class="btn btn-primary btn-sm" style="color:#fff">Edit</a>
  </div>
</div>
<div class="card">
  <div class="card-hd"><div class="card-ico">&#8505;</div><span class="card-title">Release Info</span></div>
  <div class="card-body">
    <div class="info-grid">
      <div class="info-item"><label>Type</label><span>{{ release.release_type }}</span></div>
      <div class="info-item"><label>Release Date</label><span>{{ release.release_date.strftime('%B %d, %Y') if release.release_date else '--' }}</span></div>
      <div class="info-item"><label>Distributor</label><span>{{ release.distributor or '--' }}</span></div>
      <div class="info-item"><label>UPC</label><span style="font-family:var(--fm)">{{ release.upc or 'Pending' }}</span></div>
      <div class="info-item"><label>Status</label><span class="status s-{{ release.status }}"><span class="status-dot"></span>{{ release.status | title }}</span></div>
      <div class="info-item"><label>Total Tracks</label><span>{{ release.tracks|length }}</span></div>
    </div>
    {% if release.artists_list %}
    <div style="margin-top:14px">
      <div class="label" style="margin-bottom:6px">Artists</div>
      <div style="display:flex;flex-wrap:wrap;gap:6px">
        {% for a in release.artists_list %}<span class="tag tag-full">{{ a }}</span>{% endfor %}
      </div>
    </div>
    {% endif %}
  </div>
</div>
<div class="card">
  <div class="card-hd"><div class="card-ico">&#127925;</div><span class="card-title">Tracks</span></div>
  <div class="tbl-wrap">
    <table class="tbl">
      <thead><tr><th>#</th><th>Title</th><th>Artists</th><th>Duration</th><th>ISRC</th><th>Genre</th><th>Linked Works</th></tr></thead>
      <tbody>
        {% for t in release.tracks %}
        <tr>
          <td style="color:var(--t3)">{{ t.track_number or '—' }}</td>
          <td>
            <div style="font-weight:600">{{ t.primary_title }}</div>
            {% if t.recording_title and t.recording_title != t.primary_title %}<div style="font-size:11px;color:var(--t3)">Rec: {{ t.recording_title }}</div>{% endif %}
            {% if t.aka_title %}<div style="font-size:11px;color:var(--t3)">AKA: {{ t.aka_title }}</div>{% endif %}
          </td>
          <td style="font-size:12px;color:var(--t2)">{{ t.artist_display }}</td>
          <td style="font-size:12px;color:var(--t2)">{{ t.duration or '--' }}</td>
          <td style="font-family:var(--fm);font-size:12px;color:var(--t2)">{{ t.isrc or 'Pending' }}</td>
          <td style="font-size:12px;color:var(--t2)">{{ t.genre or '--' }}</td>
          <td>
            {% for tw in t.track_works %}
            <div style="font-size:12px">
              <span class="tag tag-s1">{{ tw.work.title }}</span>
              {% for ww in tw.work.work_writers %}
              <span style="font-size:11px;color:var(--t3)">{{ ww.writer.full_name }} {{ "%.0f"|format(ww.writer_percentage) }}%</span>
              {% endfor %}
            </div>
            {% endfor %}
            {% if not t.track_works %}<span style="color:var(--t3);font-size:12px">Not linked</span>{% endif %}
          </td>
        </tr>
        {% endfor %}
        {% if not release.tracks %}<tr class="empty"><td colspan="7">No tracks yet.</td></tr>{% endif %}
      </tbody>
    </table>
  </div>
</div>
</div>
</main>
</div>
""" + _SB_JS + """
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>🖋️</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>🗒️</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>💿</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>👥</span><small>Writers</small></a>
</div>
</body></html>"""


# ================================================================
# ARTIST DIRECTORY LIST HTML
# ================================================================

ARTISTS_LIST_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Artist Directory - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("artists_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#127908;</div>
    <div>
      <div class="ph-title">Artist Directory</div>
      <div class="ph-sub">Recording artists linked to releases</div>
    </div>
  </div>
  <div class="ph-actions" style="flex-shrink:0">
    <a href="/artists/new" class="btn btn-primary">+ New Artist</a>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#128269;</div><span class="card-title">Search</span></div>
  <div class="card-body">
    <form method="get" style="display:flex;gap:8px;flex-wrap:wrap">
      <input class="inp" name="q" value="{{ q }}" placeholder="Search by name, AKA, or email..." style="max-width:340px">
      <select class="inp" name="sort" style="max-width:220px">
        <option value="newest" {% if sort == 'newest' %}selected{% endif %}>Newest First</option>
        <option value="oldest" {% if sort == 'oldest' %}selected{% endif %}>Oldest First</option>
        <option value="name_asc" {% if sort == 'name_asc' %}selected{% endif %}>Name A-Z</option>
        <option value="name_desc" {% if sort == 'name_desc' %}selected{% endif %}>Name Z-A</option>
      </select>
      <button class="btn btn-sec" type="submit">Apply</button>
      {% if q or sort not in ('newest', '') %}<a href="/artists" class="btn btn-sec">Clear</a>{% endif %}
    </form>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#127908;</div><span class="card-title">All Artists</span></div>
  <div class="tbl-wrap">
    <table class="tbl" style="table-layout:auto">
      <thead>
        <tr>
          <th style="width:35%">Artist</th>
          <th>Email</th>
          <th>Phone</th>
          <th>Releases</th>
        </tr>
      </thead>
      <tbody>
        {% for artist in artists %}
        <tr class="ar-row" data-artist="{{ artist.id }}" onclick="toggleArtist({{ artist.id }})">
          <td>
            <span class="expand-chevron">&#9658;</span>
            <span style="font-weight:600">{{ artist.name }}</span>
            {% if artist.aka %}<div style="font-size:11px;color:var(--t3);margin-top:2px;margin-left:16px">aka {{ artist.aka }}</div>{% endif %}
          </td>
          <td style="font-size:12px;color:var(--t2)">{{ artist.email or '--' }}</td>
          <td style="font-size:12px;color:var(--t2)">{{ artist.phone_number or '--' }}</td>
          <td><span style="background:rgba(99,133,255,.1);color:var(--a);border:1px solid rgba(99,133,255,.2);border-radius:99px;padding:2px 8px;font-size:11px;font-weight:700">{{ artist.release_count }}</span></td>
        </tr>
        <tr class="ar-detail-row" id="ardetail-{{ artist.id }}">
          <td colspan="4">
            <div class="ar-detail-inner">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px">
                <div style="font-size:13px;display:flex;gap:16px;flex-wrap:wrap;color:var(--t2)">
                  {% if artist.email %}<span>&#9993; {{ artist.email }}</span>{% endif %}
                  {% if artist.phone_number %}<span>&#128222; {{ artist.phone_number }}</span>{% endif %}
                  {% if artist.legal_name %}<span style="color:var(--t3)">Legal: {{ artist.legal_name }}</span>{% endif %}
                </div>
                <div style="display:flex;gap:8px">
                  <a href="/artists/{{ artist.id }}/edit" class="btn btn-primary btn-sm" style="color:#fff" onclick="event.stopPropagation()">Edit</a>
                  <a href="/artists/{{ artist.id }}" class="btn btn-sec btn-sm" onclick="event.stopPropagation()">Full View</a>
                </div>
              </div>
              {% if artist.releases %}
              <table class="wd-writers-tbl" style="width:100%;border-collapse:collapse">
                <thead>
                  <tr style="border-bottom:1px solid var(--b1)">
                    <th style="text-align:left;font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--t3);padding:4px 8px 6px 0">Title</th>
                    <th style="text-align:left;font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--t3);padding:4px 8px 6px 0">Type</th>
                    <th style="text-align:left;font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--t3);padding:4px 8px 6px 0">Release Date</th>
                    <th style="text-align:left;font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--t3);padding:4px 8px 6px 0">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {% for r in artist.releases_list %}
                  <tr style="border-bottom:1px solid var(--b0)">
                    <td style="padding:6px 8px 6px 0">
                      <a href="/releases/{{ r.id }}" style="font-weight:600;font-size:13px;color:var(--a)" onclick="event.stopPropagation()">{{ r.title }}</a>
                      <div style="font-size:11px;color:var(--t3)">{{ r.artist_display }}</div>
                    </td>
                    <td style="padding:6px 8px 6px 0;font-size:12px;color:var(--t2)">{{ r.release_type }}</td>
                    <td style="padding:6px 8px 6px 0;font-size:12px;color:var(--t2)">{{ r.release_date.strftime('%b %d, %Y') if r.release_date else '--' }}</td>
                    <td style="padding:6px 0">
                      {% if r.status == 'delivered' %}<span class="tag tag-s1">Delivered</span>
                      {% elif r.status == 'ready' %}<span class="tag tag-s2">Ready</span>
                      {% else %}<span class="tag tag-full">Draft</span>{% endif %}
                    </td>
                  </tr>
                  {% endfor %}
                </tbody>
              </table>
              {% else %}
              <div style="font-size:13px;color:var(--t3)">No releases linked to this artist yet.</div>
              {% endif %}
            </div>
          </td>
        </tr>
        {% endfor %}
        {% if not artists %}
          <tr class="empty"><td colspan="4">No artists found{% if q %} for "{{ q }}"{% endif %}.</td></tr>
        {% endif %}
      </tbody>
    </table>
  </div>
  {% if pagination.pages > 1 %}
  <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-top:1px solid var(--b1);font-size:13px;color:var(--t2)">
    <span>{{ pagination.total }} artists &mdash; page {{ pagination.page }} of {{ pagination.pages }}</span>
    <div style="display:flex;gap:6px">
      {% if pagination.has_prev %}<a href="?q={{ q }}&sort={{ sort }}&page={{ pagination.prev_num }}" class="btn btn-sec btn-sm">&#8592; Prev</a>{% endif %}
      {% if pagination.has_next %}<a href="?q={{ q }}&sort={{ sort }}&page={{ pagination.next_num }}" class="btn btn-sec btn-sm">Next &#8594;</a>{% endif %}
    </div>
  </div>
  {% endif %}
</div>
</div>
</main>
</div>
""" + _SB_JS + """
<script>
function toggleArtist(id) {
  var row = document.getElementById('ardetail-' + id);
  var header = document.querySelector('[data-artist="' + id + '"]');
  var isOpen = row.classList.contains('open');
  document.querySelectorAll('.ar-detail-row.open').forEach(function(r){ r.classList.remove('open'); });
  document.querySelectorAll('.ar-row.open').forEach(function(r){ r.classList.remove('open'); });
  if (!isOpen) {
    row.classList.add('open');
    header.classList.add('open');
    row.scrollIntoView({behavior:'smooth', block:'nearest'});
  }
}
</script>
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>&#128395;</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>&#128466;</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>&#128191;</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>&#128101;</span><small>Writers</small></a>
</div>
</body></html>"""


# ================================================================
# ARTIST DETAIL HTML
# ================================================================

ARTIST_DETAIL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ artist.name }} - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("artists_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#127908;</div>
    <div>
      <div class="ph-title">{{ artist.name }}</div>
      <div class="ph-sub">{{ artist.aka or 'Artist profile' }}</div>
    </div>
  </div>
  <div class="ph-actions">
    <a href="/artists/{{ artist.id }}/edit" class="btn btn-primary btn-sm">Edit Artist</a>
    <a href="/artists" class="btn btn-sec btn-sm">Back</a>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#127908;</div><span class="card-title">Artist Info</span></div>
  <div class="card-body">
    <div class="g g3" style="gap:8px 22px;margin-bottom:10px">
      <div class="info-item"><label>Stage Name</label><span>{{ artist.name }}</span></div>
      <div class="info-item"><label>Legal Name</label><span>{{ artist.legal_name or '--' }}</span></div>
      <div class="info-item"><label>AKA</label><span>{{ artist.aka or '--' }}</span></div>
    </div>
    <div class="g g4" style="gap:8px 22px;margin-bottom:10px">
      <div class="info-item"><label>Email</label><span>{{ artist.email or '--' }}</span></div>
      <div class="info-item"><label>Phone</label><span>{{ artist.phone_number or '--' }}</span></div>
      <div class="info-item"><label>City</label><span>{{ artist.city or '--' }}</span></div>
      <div class="info-item"><label>State</label><span>{{ artist.state or '--' }}</span></div>
    </div>
    <div class="g g2" style="gap:8px 22px;margin-bottom:10px">
      <div class="info-item"><label>Address</label><span>{{ artist.address or '--' }}</span></div>
      <div class="info-item"><label>Zip</label><span>{{ artist.zip_code or '--' }}</span></div>
    </div>
    <div class="g g2" style="gap:8px 22px">
      <div class="info-item"><label>Added</label><span>{{ artist.created_at.strftime('%b %d, %Y') if artist.created_at else '--' }}</span></div>
      <div class="info-item"><label>Last Updated</label><span>{{ artist.updated_at.strftime('%b %d, %Y') if artist.updated_at else '--' }}</span></div>
    </div>
  </div>
</div>

<div class="card">
  <div class="card-hd"><div class="card-ico">&#128191;</div><span class="card-title">Releases</span></div>
  <div class="tbl-wrap">
    <table class="tbl" style="table-layout:auto">
      <thead>
        <tr>
          <th>Title</th>
          <th>Type</th>
          <th>Release Date</th>
          <th>Status</th>
          <th>Tracks</th>
        </tr>
      </thead>
      <tbody>
        {% for r in releases %}
        <tr onclick="window.location='/releases/{{ r.id }}'" style="cursor:pointer">
          <td>
            <span style="font-weight:600">{{ r.title }}</span>
            <div style="font-size:11px;color:var(--t3);margin-top:2px">{{ r.artist_display }}</div>
          </td>
          <td style="font-size:12px;color:var(--t2)">{{ r.release_type }}</td>
          <td style="font-size:12px;color:var(--t2)">{{ r.release_date.strftime('%b %d, %Y') if r.release_date else '--' }}</td>
          <td>
            {% if r.status == 'delivered' %}<span class="tag tag-s1">Delivered</span>
            {% elif r.status == 'ready' %}<span class="tag tag-s2">Ready</span>
            {% else %}<span class="tag tag-full">Draft</span>{% endif %}
          </td>
          <td><span style="background:rgba(99,133,255,.1);color:var(--a);border:1px solid rgba(99,133,255,.2);border-radius:99px;padding:2px 8px;font-size:11px;font-weight:700">{{ r.num_tracks or r.tracks|length }}</span></td>
        </tr>
        {% endfor %}
        {% if not releases %}
          <tr class="empty"><td colspan="5">No releases linked to this artist.</td></tr>
        {% endif %}
      </tbody>
    </table>
  </div>
</div>
</div>
</main>
</div>
""" + _SB_JS + """
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>&#128395;</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>&#128466;</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>&#128191;</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>&#128101;</span><small>Writers</small></a>
</div>
</body></html>"""


# ================================================================
# ARTIST FORM HTML (Create / Edit)
# ================================================================

ARTIST_FORM_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{% if artist %}Edit {{ artist.name }}{% else %}New Artist{% endif %} - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("artists_list") + """
<main class="main">
""" + _topbar() + """
<div class="page">
{% with messages = get_flashed_messages() %}
{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}

<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#9998;</div>
    <div>
      <div class="ph-title">{% if artist %}Edit Artist{% else %}New Artist{% endif %}</div>
      <div class="ph-sub">{% if artist %}{{ artist.name }}{% else %}Add a new recording artist{% endif %}</div>
    </div>
  </div>
  <div class="ph-actions">
    {% if artist %}
    <a href="/artists/{{ artist.id }}" class="btn btn-sec btn-sm">Back to Profile</a>
    {% else %}
    <a href="/artists" class="btn btn-sec btn-sm">Back</a>
    {% endif %}
  </div>
</div>

<form method="post">
  <div class="card">
    <div class="card-hd">
      <div class="card-ico">&#127908;</div>
      <span class="card-title">Artist Information</span>
    </div>
    <div class="card-body">

      <div class="g g3" style="margin-bottom:12px">
        <div class="field">
          <label class="label">Stage Name <span style="color:var(--err)">*</span></label>
          <input class="inp" name="name" value="{{ artist.name if artist else '' }}" required>
        </div>
        <div class="field">
          <label class="label">Legal Name</label>
          <input class="inp" name="legal_name" value="{{ artist.legal_name if artist else '' }}">
        </div>
        <div class="field">
          <label class="label">AKA</label>
          <input class="inp" name="aka" value="{{ artist.aka if artist else '' }}">
        </div>
      </div>

      <div class="g g2" style="margin-bottom:12px">
        <div class="field">
          <label class="label">Email</label>
          <input class="inp" name="email" type="email" value="{{ artist.email if artist else '' }}">
        </div>
        <div class="field">
          <label class="label">Phone Number</label>
          <input class="inp" name="phone_number" value="{{ artist.phone_number if artist else '' }}">
        </div>
      </div>

      <div class="g g4a">
        <div class="field">
          <label class="label">Street Address</label>
          <input class="inp" name="address" value="{{ artist.address if artist else '' }}">
        </div>
        <div class="field">
          <label class="label">City</label>
          <input class="inp" name="city" value="{{ artist.city if artist else '' }}">
        </div>
        <div class="field">
          <label class="label">State</label>
          <input class="inp" name="state" value="{{ artist.state if artist else '' }}">
        </div>
        <div class="field">
          <label class="label">Zip</label>
          <input class="inp" name="zip_code" value="{{ artist.zip_code if artist else '' }}">
        </div>
      </div>

    </div>
  </div>

  <div class="ph-actions" style="justify-content:flex-end;margin-bottom:8px">
    <button type="submit" class="btn btn-primary">{% if artist %}Save Changes{% else %}Create Artist{% endif %}</button>
  </div>

</form>

{% if artist %}
<div style="margin-top:24px;padding-top:16px;border-top:1px solid var(--b1);text-align:right">
  <form method="post" action="/artists/{{ artist.id }}/delete" onsubmit="return confirm('Delete this artist? This cannot be undone.')">
    <button type="submit" class="btn btn-sec btn-sm" style="color:var(--err);border-color:var(--err)">Delete Artist</button>
  </form>
</div>
{% endif %}

</div>
</main>
</div>
""" + _SB_JS + """
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>&#128395;</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>&#128466;</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>&#128191;</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>&#128101;</span><small>Writers</small></a>
</div>
</body></html>"""

# ================================================================
# CATALOG CSV IMPORT
# ================================================================

CATALOG_IMPORT_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Import Catalog CSV - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("admin") + """
<main class="main">
""" + _topbar() + """
<div class="page">

{% with messages = get_flashed_messages() %}
{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}

<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128228;</div>
    <div>
      <div class="ph-title">Import Catalog CSV</div>
      <div class="ph-sub">Bulk-load releases, tracks, artists, and publishing works from your catalog file.</div>
    </div>
  </div>
  <div class="ph-actions">
    <a href="/admin" class="btn btn-sec btn-sm">Back to Admin</a>
  </div>
</div>

<div class="card">
  <div class="card-hd">
    <div class="card-ico">&#128202;</div>
    <span class="card-title">Upload Catalog CSV</span>
  </div>
  <div class="card-body">
    <form method="post" action="/admin/import-catalog-csv" enctype="multipart/form-data">
      <div class="field" style="margin-bottom:16px">
        <label class="label">CSV File</label>
        <input class="inp" type="file" name="catalog_file" accept=".csv" required>
      </div>
      <div style="color:var(--t2);font-size:12px;margin-bottom:16px;line-height:1.7">
        <strong>Expected format:</strong> one row per track with columns UPC, Album Title, Artist, Track title, ISRC,
        Composer 1&ndash;8 (+ IPI, Split %, PRO), Publisher 1&ndash;7, and a <em>Publishing</em> TRUE/FALSE flag.<br>
        Rows with <strong>Publishing = TRUE</strong> will create Works in the publishing catalog.<br>
        Rows with <strong>Publishing = FALSE</strong> will create Release + Track only (covers, etc.).<br>
        The import is <strong>idempotent</strong> &mdash; safe to re-run without duplicating data.
      </div>
      <button type="submit" class="btn btn-primary">Upload &amp; Import</button>
    </form>
  </div>
</div>

</div>
</main>
</div>
""" + _SB_JS + """
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>&#128395;</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>&#128466;</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>&#128191;</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>&#128101;</span><small>Writers</small></a>
</div>
</body></html>"""


CATALOG_IMPORT_RESULT_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Import Results - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("admin") + """
<main class="main">
""" + _topbar() + """
<div class="page">

<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#9989;</div>
    <div>
      <div class="ph-title">Import Complete</div>
      <div class="ph-sub">Catalog CSV has been processed.</div>
    </div>
  </div>
  <div class="ph-actions">
    <a href="/releases" class="btn btn-primary btn-sm">View Releases</a>
    <a href="/admin/import-catalog-csv" class="btn btn-sec btn-sm" style="margin-left:8px">Import Another</a>
  </div>
</div>

<div class="card">
  <div class="card-hd">
    <div class="card-ico">&#128202;</div>
    <span class="card-title">Summary</span>
  </div>
  <div class="card-body">
    <div class="info-grid">
      <div class="info-item"><label>Releases Created</label><span>{{ stats.releases_created }}</span></div>
      <div class="info-item"><label>Releases Updated</label><span>{{ stats.releases_updated }}</span></div>
      <div class="info-item"><label>Tracks Created</label><span>{{ stats.tracks_created }}</span></div>
      <div class="info-item"><label>Tracks Updated</label><span>{{ stats.tracks_updated }}</span></div>
      <div class="info-item"><label>Artists Created</label><span>{{ stats.artists_created }}</span></div>
      <div class="info-item"><label>Works Created</label><span>{{ stats.works_created }}</span></div>
      <div class="info-item"><label>Writers Created</label><span>{{ stats.writers_created }}</span></div>
      <div class="info-item"><label>Rows Skipped</label><span>{{ stats.rows_skipped }}</span></div>
      <div class="info-item"><label>Errors</label><span style="{% if stats.errors %}color:#ff8a8a{% endif %}">{{ stats.errors|length }}</span></div>
    </div>
  </div>
</div>

{% if stats.errors %}
<div class="card">
  <div class="card-hd">
    <div class="card-ico">&#9888;</div>
    <span class="card-title">Errors ({{ stats.errors|length }})</span>
  </div>
  <div class="card-body">
    <div style="font-family:var(--fm);font-size:12px;color:#ff8a8a;line-height:1.9">
      {% for e in stats.errors %}
      <div>{{ loop.index }}. {{ e }}</div>
      {% endfor %}
    </div>
  </div>
</div>
{% endif %}

</div>
</main>
</div>
""" + _SB_JS + """
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>&#128395;</span><small>Works</small></a>
  <a href="/batches" class="mnav-item"><span>&#128466;</span><small>Sessions</small></a>
  <a href="/releases" class="mnav-item"><span>&#128191;</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>&#128101;</span><small>Writers</small></a>
</div>
</body></html>"""


# ── Phase 3: Reports Pages ─────────────────────────────────────────────────────

REPORTS_INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Reports - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("reports") + """
<main class="main">
""" + _topbar() + """
<div class="page">
{% with messages = get_flashed_messages() %}{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#128202;</div>
    <div><div class="ph-title">Reports</div><div class="ph-sub">Export catalog data for MLC, Music Reports, and SoundExchange</div></div>
  </div>
  <div class="ph-actions">
    <a href="/publisher-config" class="btn btn-sec">Publisher Config</a>
    <a href="/pro-registration" class="btn btn-sec">PRO Registration</a>
  </div>
</div>
<div class="g g2" style="gap:14px">
  <div class="card">
    <div class="card-hd"><span style="font-size:18px;margin-right:4px">&#128203;</span><span class="card-title">MLC Bulk Work Registration</span></div>
    <div class="card-body" style="padding:16px">
      <p style="font-size:13px;color:var(--t2);margin-bottom:12px">Exports all controlled works (Songs / Melodies / Music of Afinarte) in the MLC portal bulk upload format. One row per writer per work.</p>
      <div style="font-size:12px;color:var(--t3);margin-bottom:14px"><strong style="color:var(--t2)">{{ work_count }}</strong> controlled works in catalog</div>
      <a href="/reports/export/mlc" class="btn btn-primary" style="width:100%;text-align:center;display:block">Download MLC Export (.xlsx)</a>
    </div>
  </div>
  <div class="card">
    <div class="card-hd"><span style="font-size:18px;margin-right:4px">&#127925;</span><span class="card-title">Music Reports Catalog</span></div>
    <div class="card-body" style="padding:16px">
      <p style="font-size:13px;color:var(--t2);margin-bottom:12px">Exports controlled works in Music Reports (MRI) format including composer, publisher, territory, and linked recording data.</p>
      <div style="font-size:12px;color:var(--t3);margin-bottom:14px"><strong style="color:var(--t2)">{{ work_count }}</strong> controlled works in catalog</div>
      <a href="/reports/export/music-reports" class="btn btn-primary" style="width:100%;text-align:center;display:block">Download Music Reports Export (.xls)</a>
    </div>
  </div>
  <div class="card">
    <div class="card-hd"><span style="font-size:18px;margin-right:4px">&#128266;</span><span class="card-title">SoundExchange ISRC Ingest</span></div>
    <div class="card-body" style="padding:16px">
      <p style="font-size:13px;color:var(--t2);margin-bottom:12px">Exports <strong style="color:var(--t1)">all releases</strong> (controlled and uncontrolled) with ISRC, artist, genre, and duration for SoundExchange.</p>
      <div style="font-size:12px;color:var(--t3);margin-bottom:14px"><strong style="color:var(--t2)">{{ release_count }}</strong> releases in catalog</div>
      <a href="/reports/export/soundexchange" class="btn btn-primary" style="width:100%;text-align:center;display:block">Download SoundExchange Export (.xlsx)</a>
    </div>
  </div>
  <div class="card" style="border:1px dashed var(--b0);opacity:.55">
    <div class="card-hd"><span style="font-size:18px;margin-right:4px">&#128200;</span><span class="card-title" style="color:var(--t2)">Regalias Digitales</span></div>
    <div class="card-body" style="padding:16px">
      <p style="font-size:13px;color:var(--t3);margin-bottom:14px">Template coming soon. Once the format is defined this export will be available here.</p>
      <div class="btn btn-sec" style="width:100%;text-align:center;opacity:.5;cursor:default;pointer-events:none">Coming Soon</div>
    </div>
  </div>
</div>
<div style="margin-top:12px"><a href="/pro-registration" class="btn btn-sec">PRO Registration Queue &rarr;</a></div>
</div></main></div>
""" + _SB_JS + """
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>&#128395;</span><small>Works</small></a>
  <a href="/reports" class="mnav-item on"><span>&#128202;</span><small>Reports</small></a>
  <a href="/releases" class="mnav-item"><span>&#128191;</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>&#128101;</span><small>Writers</small></a>
</div>
</body></html>"""


PUBLISHER_CONFIG_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Publisher Config - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("reports") + """
<main class="main">
""" + _topbar() + """
<div class="page">
{% with messages = get_flashed_messages() %}{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#127970;</div>
    <div><div class="ph-title">Publisher Configuration</div><div class="ph-sub">Set up your Afinarte publisher entities for report exports</div></div>
  </div>
</div>
<form method="post" autocomplete="off">
{% for pc in configs %}
<div class="card" style="margin-bottom:16px">
  <div class="card-hd">
    <span style="font-size:14px;font-weight:700;color:var(--t1)">{{ pc.publisher_name }}</span>
    <span style="margin-left:auto;font-size:10px;padding:2px 8px;border-radius:20px;background:rgba(99,133,255,.15);color:#6385ff;font-weight:700">Afinarte Publisher</span>
  </div>
  <div style="padding:16px">
    <input type="hidden" name="pub_id[]" value="{{ pc.id }}">
    <input type="hidden" name="publisher_name[]" value="{{ pc.publisher_name }}">
    <div class="g g3" style="gap:12px;margin-bottom:12px">
      <div class="field"><label class="label">PRO Affiliation</label>
        <select class="inp" name="pro[]">
          <option value="">-- Select --</option>
          {% for p in ['BMI','ASCAP','SESAC'] %}<option value="{{ p }}" {% if pc.pro == p %}selected{% endif %}>{{ p }}</option>{% endfor %}
        </select>
      </div>
      <div class="field"><label class="label">Publisher IPI #</label>
        <input class="inp" name="publisher_ipi[]" value="{{ pc.publisher_ipi or \'\' }}" placeholder="e.g. 00123456789">
      </div>
      <div class="field"><label class="label">MLC Publisher #</label>
        <input class="inp" name="mlc_publisher_number[]" value="{{ pc.mlc_publisher_number or \'\' }}" placeholder="e.g. P12345">
      </div>
    </div>
    <div class="g g3" style="gap:12px;margin-bottom:12px">
      <div class="field" style="grid-column:span 2"><label class="label">Mailing Address</label>
        <input class="inp" name="address[]" value="{{ pc.address or \'\' }}" placeholder="Street Address">
      </div>
      <div class="field"><label class="label">City</label>
        <input class="inp" name="city[]" value="{{ pc.city or \'\' }}" placeholder="City">
      </div>
    </div>
    <div class="g g3" style="gap:12px;margin-bottom:12px">
      <div class="field"><label class="label">State</label>
        <input class="inp" name="state[]" value="{{ pc.state or \'\' }}" placeholder="CA">
      </div>
      <div class="field"><label class="label">Zip</label>
        <input class="inp" name="zip_code[]" value="{{ pc.zip_code or \'\' }}" placeholder="90001">
      </div>
      <div class="field"><label class="label">Contact Email</label>
        <input class="inp" name="contact_email[]" value="{{ pc.contact_email or \'\' }}" placeholder="publishing@afinarte.com">
      </div>
    </div>
    <div class="g g2" style="gap:12px">
      <div class="field"><label class="label">Contact Phone</label>
        <input class="inp" name="contact_phone[]" value="{{ pc.contact_phone or \'\' }}" placeholder="(555) 555-5555">
      </div>
    </div>
  </div>
</div>
{% endfor %}
<div class="action-bar">
  <button type="submit" class="btn btn-primary">Save All</button>
  <a href="/reports" class="btn btn-sec">Back to Reports</a>
</div>
</form>
</div></main></div>
""" + _SB_JS + """
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>&#128395;</span><small>Works</small></a>
  <a href="/reports" class="mnav-item on"><span>&#128202;</span><small>Reports</small></a>
  <a href="/releases" class="mnav-item"><span>&#128191;</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>&#128101;</span><small>Writers</small></a>
</div>
</body></html>"""


PRO_REGISTRATION_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>PRO Registration - LabelMind</title>""" + _STYLE + """
</head>
<body>
<div class="app" id="mainApp">
""" + _sidebar("pro_registration") + """
<main class="main">
""" + _topbar() + """
<div class="page">
{% with messages = get_flashed_messages() %}{% if messages %}
<div class="flash-list">{% for m in messages %}<div class="flash-item">&#9888; {{ m }}</div>{% endfor %}</div>
{% endif %}{% endwith %}
<div class="ph">
  <div class="ph-left">
    <div class="ph-icon">&#9989;</div>
    <div><div class="ph-title">PRO Registration</div><div class="ph-sub">Track controlled works registered with BMI, ASCAP, or SESAC</div></div>
  </div>
  <div class="ph-actions"><a href="/reports" class="btn btn-sec">Back to Reports</a></div>
</div>
<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;align-items:center">
  <a href="/pro-registration?tab=unregistered{% if q %}&amp;q={{ q }}{% endif %}"
     class="pill {% if tab == \'unregistered\' %}on{% endif %}">Unregistered ({{ unregistered_count }})</a>
  <a href="/pro-registration?tab=registered{% if q %}&amp;q={{ q }}{% endif %}"
     class="pill {% if tab == \'registered\' %}on{% endif %}">Registered ({{ registered_count }})</a>
  <form method="get" style="margin-left:auto;display:flex;gap:8px">
    <input type="hidden" name="tab" value="{{ tab }}">
    <input class="inp" name="q" value="{{ q }}" placeholder="Search worksâ¦" style="width:200px">
    <button class="btn btn-sec" type="submit">Search</button>
  </form>
</div>
{% if tab == \'unregistered\' %}
<form method="post" action="/pro-registration/mark" id="markForm">
<div class="card">
  <div class="card-hd"><span class="card-title">Unregistered Controlled Works</span>
    <span style="font-size:11px;color:var(--t3);margin-left:8px">(publisher = Songs / Melodies / Music of Afinarte)</span>
  </div>
  {% if unregistered %}
  <div style="padding:14px 16px;border-bottom:1px solid var(--b0);display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end">
    <div class="field" style="min-width:130px"><label class="label">PRO *</label>
      <select class="inp" name="pro" required>
        <option value="">-- Select PRO --</option>
        <option>BMI</option><option>ASCAP</option><option>SESAC</option>
      </select>
    </div>
    <div class="field"><label class="label">Registration Date</label>
      <input class="inp" type="date" name="registered_at" value="{{ today }}">
    </div>
    <div class="field"><label class="label">Registered By</label>
      <input class="inp" name="registered_by" value="Omar">
    </div>
    <div class="field" style="min-width:160px"><label class="label">PRO Work # <span style="color:var(--t3)">(optional)</span></label>
      <input class="inp" name="pro_work_number" placeholder="Assigned by PRO after submission">
    </div>
    <div class="field" style="min-width:130px"><label class="label">MLC Song Code <span style="color:var(--t3)">(optional)</span></label>
      <input class="inp" name="mlc_song_code" placeholder="e.g. H12345">
    </div>
    <div class="field" style="min-width:160px"><label class="label">Notes</label>
      <input class="inp" name="notes" placeholder="Optional">
    </div>
  </div>
  <div style="overflow-x:auto">
  <table class="tbl" style="width:100%">
    <thead><tr>
      <th style="width:36px"><input type="checkbox" id="chkAll" onclick="document.querySelectorAll(\'.work-chk\').forEach(function(c){c.checked=this.checked;},this)"></th>
      <th>Work Title</th>
      <th>Writers</th>
      <th>Publisher</th>
      <th>Contract Date</th>
    </tr></thead>
    <tbody>
    {% for w in unregistered %}
    <tr class="work-row" data-pro-work="{{ w.id }}" onclick="toggleProWork({{ w.id }}, event)" style="cursor:pointer">
      <td onclick="event.stopPropagation()"><input type="checkbox" name="work_ids[]" value="{{ w.id }}" class="work-chk"></td>
      <td>
        <span class="expand-chevron">&#9658;</span>
        <span style="font-weight:600">{{ w.title }}</span>
        {% if w.aka_title %}<div style="font-size:11px;color:var(--t3);margin-left:16px;margin-top:2px">AKA: {{ w.aka_title }}</div>{% endif %}
      </td>
      <td style="font-size:12px;color:var(--t2)">
        {% for ww in w.work_writers[:2] %}{{ ww.writer.full_name }}{% if not loop.last %}, {% endif %}{% endfor %}
        {% if w.work_writers|length > 2 %} +{{ w.work_writers|length - 2 }} more{% endif %}
      </td>
      <td style="font-size:12px;color:var(--t2)">
        {% for ww in w.work_writers %}{% if ww.publisher in [\'Songs of Afinarte\',\'Melodies of Afinarte\',\'Music of Afinarte\'] %}<span style="display:block">{{ ww.publisher }}</span>{% endif %}{% endfor %}
      </td>
      <td style="font-size:12px;color:var(--t3)">{{ w.contract_date.strftime(\'%m/%d/%Y\') if w.contract_date else \'\xe2\x80\x94\' }}</td>
    </tr>
    <tr class="work-detail-row" id="pro-detail-{{ w.id }}">
      <td colspan="5">
        <div class="work-detail-inner" style="grid-template-columns:1.2fr 2.8fr 1fr;padding-left:36px">
          <div class="wd-section">
            <div class="wd-label">Work Info</div>
            <div style="display:grid;grid-template-columns:auto 1fr;gap:4px 12px;font-size:12px">
              <span style="color:var(--t3)">Title</span><span style="color:var(--t1);font-weight:600">{{ w.title }}</span>
              <span style="color:var(--t3)">AKA</span><span style="color:var(--t2)">{{ w.aka_title or \'\xe2\x80\x94\' }}</span>
              <span style="color:var(--t3)">ISWC</span><span style="color:var(--t2);font-family:var(--fm)">{{ w.iswc or \'\xe2\x80\x94\' }}</span>
              <span style="color:var(--t3)">Duration</span><span style="color:var(--t2)">{{ w._first_track.duration if w._first_track and w._first_track.duration else \'\xe2\x80\x94\' }}</span>
              <span style="color:var(--t3)">Recording Title</span><span style="color:var(--t2)">{{ w._first_track.recording_title if w._first_track and w._first_track.recording_title else (w._first_track.primary_title if w._first_track else \'\xe2\x80\x94\') }}</span>
            </div>
          </div>
          <div class="wd-section">
            <div class="wd-label">Writers &amp; Publishers</div>
            <table class="wd-writers-tbl" style="width:100%">
              <thead><tr><th>Writer</th><th>IPI</th><th>PRO</th><th>Share</th><th>Publisher</th><th>Pub IPI</th></tr></thead>
              <tbody>
              {% for ww in w.work_writers %}
              <tr>
                <td style="font-weight:600">{{ ww.writer.full_name }}</td>
                <td style="font-family:var(--fm)">{{ ww.writer.ipi or \'\xe2\x80\x94\' }}</td>
                <td>{{ ww.writer.pro or \'\xe2\x80\x94\' }}</td>
                <td style="color:var(--a);font-weight:700">{{ "%.2f"|format(ww.writer_percentage) }}%</td>
                <td style="font-size:11px;color:var(--t2)">{{ ww.publisher or \'\xe2\x80\x94\' }}</td>
                <td style="font-family:var(--fm);font-size:11px;color:var(--t3)">{{ ww.publisher_ipi or \'\xe2\x80\x94\' }}</td>
              </tr>
              {% endfor %}
              </tbody>
            </table>
          </div>
          <div class="wd-section">
            <div class="wd-label">Recording &amp; Release</div>
            {% if w._first_release %}
            <div style="display:grid;grid-template-columns:auto 1fr;gap:4px 12px;font-size:12px">
              <span style="color:var(--t3)">Release Type</span><span style="color:var(--t2)">{{ w._first_release.release_type }}</span>
              <span style="color:var(--t3)">Release Date</span><span style="color:var(--t2)">{{ w._first_release.release_date.strftime(\'%m/%d/%Y\') if w._first_release.release_date else \'\xe2\x80\x94\' }}</span>
              <span style="color:var(--t3)">Record Label</span><span style="color:var(--t2)">{{ w._first_track.track_label or \'\xe2\x80\x94\' }}</span>
              <span style="color:var(--t3)">ISRC</span><span style="color:var(--t2);font-family:var(--fm)">{{ w._first_track.isrc or \'\xe2\x80\x94\' }}</span>
              <span style="color:var(--t3)">UPC</span><span style="color:var(--t2);font-family:var(--fm)">{{ w._first_release.upc or \'\xe2\x80\x94\' }}</span>
            </div>
            {% if w._tracks|length > 1 %}
            <div style="font-size:11px;color:var(--t3);margin-top:8px">+{{ w._tracks|length - 1 }} more recording(s)</div>
            {% endif %}
            {% else %}
            <div style="font-size:12px;color:var(--t3)">No linked release</div>
            {% endif %}
          </div>
        </div>
      </td>
    </tr>
    {% endfor %}
    </tbody>
  </table>
  </div>
  <div style="padding:14px 16px;display:flex;align-items:center;gap:12px;flex-wrap:wrap">
    <button type="submit" class="btn btn-primary"
      onclick="var c=document.querySelectorAll(\'.work-chk:checked\').length;var p=document.querySelector(\'[name=pro]\').value;if(!c){alert(\'Select at least one work.\');return false;}if(!p){alert(\'Select a PRO.\');return false;}return true;">
      Mark Selected as Registered
    </button>
    {% if pagination.pages > 1 %}
    <span style="font-size:12px;color:var(--t3)">{{ pagination.total }} works &mdash; page {{ pagination.page }} of {{ pagination.pages }}</span>
    {% if pagination.has_prev %}<a href="?tab=unregistered&q={{ q }}&page={{ pagination.prev_num }}" class="btn btn-sec btn-sm">&#8592; Prev</a>{% endif %}
    {% if pagination.has_next %}<a href="?tab=unregistered&q={{ q }}&page={{ pagination.next_num }}" class="btn btn-sec btn-sm">Next &#8594;</a>{% endif %}
    {% endif %}
  </div>
  {% else %}
  <div style="padding:24px;text-align:center;color:var(--t3);font-size:13px">All controlled works have been registered. &#9989;</div>
  {% endif %}
</div>
</form>
{% else %}
<div class="card">
  <div class="card-hd"><span class="card-title">Registered Works</span></div>
  {% if registered %}
  <div style="overflow-x:auto">
  <table class="tbl" style="width:100%">
    <thead><tr>
      <th>Work Title</th><th>PRO</th><th>PRO Work #</th><th>MLC Code</th><th>Date</th><th>By</th><th></th>
    </tr></thead>
    <tbody>
    {% for w in registered %}{% for reg in w.registrations %}
    <tr>
      {% if loop.first %}<td rowspan="{{ w.registrations|length }}" style="vertical-align:top;font-weight:500;padding-top:12px">
        <a href="/works/{{ w.id }}" style="color:var(--t1)">{{ w.title }}</a></td>{% endif %}
      <td><span style="font-size:11px;font-weight:700;padding:2px 8px;border-radius:20px;background:rgba(99,133,255,.15);color:#6385ff">{{ reg.pro }}</span></td>
      <td style="font-size:12px;color:var(--t2)">{{ reg.pro_work_number or \'\xe2\x80\x94\' }}</td>
      <td style="font-size:12px;color:var(--t2)">{{ reg.mlc_song_code or \'\xe2\x80\x94\' }}</td>
      <td style="font-size:12px;color:var(--t3)">{{ reg.registered_at.strftime(\'%m/%d/%Y\') }}</td>
      <td style="font-size:12px;color:var(--t3)">{{ reg.registered_by or \'\xe2\x80\x94\' }}</td>
      <td><form method="post" action="/pro-registration/{{ reg.id }}/delete"
            onsubmit="return confirm(\'Remove this registration record?\')" style="margin:0">
          <button type="submit" class="btn btn-danger btn-xs">Remove</button></form></td>
    </tr>
    {% endfor %}{% endfor %}
    </tbody>
  </table>
  </div>
  {% if pagination.pages > 1 %}
  <div style="padding:12px 16px;display:flex;align-items:center;gap:10px;border-top:1px solid var(--b0)">
    <span style="font-size:12px;color:var(--t3)">{{ pagination.total }} works &mdash; page {{ pagination.page }} of {{ pagination.pages }}</span>
    {% if pagination.has_prev %}<a href="?tab=registered&q={{ q }}&page={{ pagination.prev_num }}" class="btn btn-sec btn-sm">&#8592; Prev</a>{% endif %}
    {% if pagination.has_next %}<a href="?tab=registered&q={{ q }}&page={{ pagination.next_num }}" class="btn btn-sec btn-sm">Next &#8594;</a>{% endif %}
  </div>
  {% endif %}
  {% else %}
  <div style="padding:24px;text-align:center;color:var(--t3);font-size:13px">No registered works yet.</div>
  {% endif %}
</div>
{% endif %}
</div></main></div>
<script>
function toggleProWork(id, e) {
  if (e && e.target && (e.target.type === 'checkbox' || e.target.tagName === 'A')) return;
  var row = document.getElementById('pro-detail-' + id);
  var hdr = document.querySelector('[data-pro-work="' + id + '"]');
  var isOpen = row.classList.contains('open');
  document.querySelectorAll('.work-detail-row.open').forEach(function(r){ r.classList.remove('open'); });
  document.querySelectorAll('.work-row.open').forEach(function(r){ r.classList.remove('open'); });
  if (!isOpen) {
    row.classList.add('open');
    hdr.classList.add('open');
    row.scrollIntoView({behavior:'smooth', block:'nearest'});
  }
}
</script>
""" + _SB_JS + """
<div class="mobile-nav">
  <a href="/works" class="mnav-item"><span>&#128395;</span><small>Works</small></a>
  <a href="/reports" class="mnav-item on"><span>&#128202;</span><small>Reports</small></a>
  <a href="/releases" class="mnav-item"><span>&#128191;</span><small>Releases</small></a>
  <a href="/writers" class="mnav-item"><span>&#128101;</span><small>Writers</small></a>
</div>
</body></html>"""
