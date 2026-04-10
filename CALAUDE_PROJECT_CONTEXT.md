# CLAUDE_PROJECT_CONTEXT.md

## Project Name
LabelMind.ai

## Purpose
LabelMind is a modular operations platform for music publishing and master recording administration.

The long-term vision is to manage:
- publishing workflows
- contracts
- track and album metadata
- PRO registration
- publishing royalty reporting
- master/streaming royalty reporting
- additional label/rights/admin modules later

---

## Current Status
We have completed **Phase 1** of the platform.

Phase 1 is the **Publishing Contracts Module**.

This module is intended to be frozen as a stable production workflow so Jorge and the A&R team can begin using it immediately without interruptions while future modules are built separately.

---

## Phase 1 Scope
The Phase 1 Publishing Contracts Module currently handles:

- login/authentication
- writer intake
- writer autocomplete/search
- duplicate writer and duplicate IPI checks
- duplicate work warning
- split validation (must equal 100%)
- session creation
- adding works into existing sessions
- work storage
- writer/work relationship storage
- contract generation
- Full Contract vs Schedule 1 logic
- Google Drive upload of generated contracts
- viewing generated contract files
- DocuSign send/resend
- DocuSign webhook processing
- saving signed PDFs
- saving signature certificates
- manual upload of signed files
- works list
- sessions list
- session detail
- work detail
- dark LabelMind UI

---

## Important Business Terminology
User-facing terminology should follow these rules:

- Use **Session** in the UI instead of Batch
- Use **New Work** for the create-work page
- Use **Works** for the works list page
- Use **Sessions** for grouped work/contract generation pages

Internal model names may remain unchanged for compatibility if needed:
- `Camp`
- `GenerationBatch`

But user-facing copy should prefer:
- Session Name
- Session
- Add to Existing Session
- Save Work to Session
- Works in Session

---

## Current Tech Stack
Current Phase 1 app stack:

- Python
- Flask
- SQLAlchemy
- PostgreSQL or SQLite fallback depending environment
- Google Drive API
- DocuSign eSignature API
- HTML templates stored inline in `app.py`
- custom CSS and vanilla JavaScript

---

## Current Data Model
Current main entities include:

- `Camp`
- `GenerationBatch`
- `Writer`
- `Work`
- `WorkWriter`
- `ContractDocument`

### Meaning of each
- `Camp`: session name/category
- `GenerationBatch`: session record
- `Writer`: songwriter/composer entity
- `Work`: composition/work
- `WorkWriter`: join table with split and publisher data
- `ContractDocument`: generated/sent/signed publishing contract record

---

## Phase 1 Stability Rule
This module should now be treated as a **production-stable module**.

### Very important:
Claude should assume:
- this module is being finalized for real users
- Jorge and A&R will use this workflow immediately
- this module should not receive risky architectural changes
- only safe bug fixes, polish, and low-risk improvements should be made here

### For this module:
Allowed changes:
- bug fixes
- UI cleanup
- wording fixes
- alignment fixes
- sidebar/menu improvements
- safer error handling
- docs / SOP help

Avoid unless explicitly requested:
- major refactors
- database schema rewrites
- moving to a new framework inside this production copy
- breaking route names
- removing working business logic
- large-scale template rewrites unless carefully controlled

---

## Freeze Strategy
Phase 1 should be frozen once the checklist is complete.

After freeze:
- keep this app stable
- use it for live operations
- build future modules elsewhere
- do not mix large future architecture work directly into the live Phase 1 production app

---

## Future Build Strategy
Future modules should be developed separately from the frozen Phase 1 production module.

Preferred long-term direction:

### Frontend
- React or Next.js

### Backend
- Python backend API
- Flask or FastAPI

### Database
- PostgreSQL

### Automation
- n8n

### File Handling
- Google Drive for now

---

## Long-Term Module Roadmap

### Phase 1
Publishing Contracts Module  
Status: built / being finalized / freeze for production

### Phase 2
Track and Album Metadata Module  
Owner: Paty workflow

This module should support:
- UPC
- ISRC
- track title
- album/release title
- artist name
- featured artists
- version info
- release type
- release date
- label/distributor data
- master ownership/control
- publishing ownership/control
- link between recordings and compositions
- storage in database

### Phase 3
PRO Registration Module  
Owner: Omar workflow

This module should support:
- review of works/compositions
- review of splits and writer data
- publishing control verification
- registration workflow tracking
- statuses like:
  - not started
  - in progress
  - submitted
  - registered
  - issue/pending

### Phase 4
Publishing Royalty Reporting Module

Should support:
- royalty ingestion
- work-level reporting
- writer-level reporting
- publisher share tracking
- statements
- exports

### Phase 5
Track / Master Streaming Royalty Reporting Module

Should support:
- track-level income reporting
- ISRC and UPC reporting
- DSP/platform reporting
- territory/source reporting
- artist/admin views

### Future Modules
Possible future modules include:
- marketing
- calendar
- events
- chat
- admin
- settings
- additional label/rights workflows

---

## Critical Business Rules
These rules are extremely important and should not be ignored.

### 1. Composition and recording are not the same
A **Work/Composition** is different from a **Track/Recording**.

### 2. Master and publishing control are separate
Some tracks are controlled on the master side but **not** on the publishing side.

Do not assume:
- every track has publishing control
- every composition has master control
- every release can be treated as both

### 3. Ownership must be modeled separately
Future architecture should clearly separate:
- publishing ownership/control
- master ownership/control

### 4. Multi-step workflow matters
Different team members handle different stages:
- contracts/publishing workflow
- metadata input
- PRO registration
- future reporting modules

### 5. Stability of live workflow matters
If a change risks interrupting Jorge or A&R’s live process, it should not go into the frozen Phase 1 module.

---

## UI / UX Preferences
The design language should stay consistent with the current LabelMind dark UI.

Preferred style:
- dark modern UI
- clean dashboard feel
- consistent spacing
- readable but compact tables
- slightly larger fields and readable font size
- reusable status badges/tags
- smooth interactions
- professional SaaS feel

### Sidebar behavior preference
The left sidebar should:
- support open and closed pinned states
- when open: show icons + labels
- when closed: show icons only
- when closed and hovered: expand temporarily
- when mouse leaves: collapse again
- use the same yellow pencil icon style for New Work as used in the New Work page header

---

## Current Known UX Preferences
- Works button should go to the Works list
- New Work should be separate
- Sessions should be visible as a main menu item
- menu should keep all planned items visible
- file name links are preferred over separate open buttons in some places
- works/session tables should remain aligned under polling/live updates

---

## Coding Preferences
Claude should follow these principles when assisting:

- preserve working business logic
- prefer minimal safe changes in the frozen module
- do not invent large rewrites unless requested
- explain where code should be pasted
- group changes by section
- identify exact replacement blocks when possible
- keep naming consistent
- avoid duplicate routes
- avoid duplicate template fragments
- keep frontend/server rendered table columns aligned
- prefer reusable helpers where safe
- preserve DocuSign and Drive integration unless explicitly changing them

---

## What Claude Should Help With
Claude is most useful for this project in these ways:

### For the frozen Phase 1 app
- bug fixing
- UI cleanup
- template cleanup
- safer JS/CSS behavior
- wording consistency
- documentation and SOP creation
- test checklist creation
- refactor advice without breaking production

### For future modules
- schema planning
- architecture planning
- component planning
- API design
- module specs
- migration strategy
- React/Next.js build planning
- workflow design for Paty/Omar stages

---

## What Claude Should Avoid
Unless explicitly requested, Claude should avoid:
- replacing the whole app unnecessarily
- mixing future architecture into the live production copy
- renaming DB structures destructively
- changing DocuSign flow without caution
- changing Google Drive flow without caution
- deleting existing working behavior just because there is a cleaner design
- assuming publishing control exists for all tracks

---

## Current Priority
Immediate priority:

1. finalize Phase 1 Publishing Contracts Module
2. freeze it for production use
3. let Jorge and A&R start using it
4. continue future module work in a separate track

---

## Development Rule Going Forward
If working on the frozen contracts app:
- prioritize stability over elegance

If working on future phases:
- prioritize scalability and modular design

---

## If Claude Suggests Architecture
Claude should recommend:

### For Phase 1 live app
- keep stable
- only safe improvements

### For future platform
- React / Next.js frontend
- Python API backend
- PostgreSQL
- modular architecture
- clear separation between:
  - works/compositions
  - tracks/recordings
  - releases/albums
  - publishing rights
  - master rights

---

## Notes
This project is being built as a real business operating system, not just a demo app.

Suggestions should be practical, scalable, and aligned with real music publishing and rights-management workflows.
