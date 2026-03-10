-- Scout Database Schema
-- Run this in Supabase SQL Editor to set up all tables

-- Enable UUID extension (usually already enabled)
create extension if not exists "pgcrypto";

-- ─────────────────────────────────────────────
-- Clients
-- ─────────────────────────────────────────────
create table if not exists clients (
  id uuid default gen_random_uuid() primary key,
  name text not null,
  slug text unique not null,
  config jsonb default '{}',
  active boolean default true,
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now()
);

-- ─────────────────────────────────────────────
-- Competitors
-- ─────────────────────────────────────────────
create table if not exists competitors (
  id uuid default gen_random_uuid() primary key,
  client_id uuid references clients(id) on delete cascade,
  name text not null,
  domain text,
  active boolean default true,
  created_at timestamp with time zone default now()
);

create index if not exists competitors_client_id_idx on competitors(client_id);
create index if not exists competitors_domain_idx on competitors(domain);

-- ─────────────────────────────────────────────
-- Signals (raw collected data)
-- ─────────────────────────────────────────────
create table if not exists signals (
  id uuid default gen_random_uuid() primary key,
  client_id uuid references clients(id) on delete cascade,
  competitor_id uuid references competitors(id) on delete cascade,
  source text not null,       -- semrush, semrush_csv, reddit, google_news, youtube, web_change
  signal_type text not null,  -- organic_keywords, paid_keywords, brand_mentions, news_mentions, etc.
  data jsonb not null default '{}',
  collected_at timestamp with time zone default now()
);

create index if not exists signals_client_id_idx on signals(client_id);
create index if not exists signals_collected_at_idx on signals(collected_at desc);
create index if not exists signals_source_type_idx on signals(source, signal_type);

-- ─────────────────────────────────────────────
-- Briefings (Claude-synthesized weekly reports)
-- ─────────────────────────────────────────────
create table if not exists briefings (
  id uuid default gen_random_uuid() primary key,
  client_id uuid references clients(id) on delete cascade,
  week_of date not null,
  pressure_score integer check (pressure_score >= 0 and pressure_score <= 100),
  summary text,
  developments jsonb default '[]',
  full_report jsonb default '{}',
  created_at timestamp with time zone default now()
);

create unique index if not exists briefings_client_week_idx on briefings(client_id, week_of);
create index if not exists briefings_client_id_idx on briefings(client_id);
create index if not exists briefings_week_of_idx on briefings(week_of desc);

-- ─────────────────────────────────────────────
-- Seed: Demo client (Apex Nutrition)
-- ─────────────────────────────────────────────
insert into clients (name, slug, config) values (
  'Apex Nutrition',
  'apex-nutrition',
  '{
    "domain": "apexnutrition.com",
    "vertical": "health_wellness",
    "competitors": [
      {"name": "Peak Fuel", "domain": "peakfuel.com"},
      {"name": "Vitagen", "domain": "vitagen.com"},
      {"name": "CoreStrength", "domain": "corestrength.com"}
    ]
  }'
) on conflict (slug) do nothing;

-- Add competitors for Apex Nutrition
insert into competitors (client_id, name, domain)
select c.id, 'Peak Fuel', 'peakfuel.com'
from clients c where c.slug = 'apex-nutrition'
on conflict do nothing;

insert into competitors (client_id, name, domain)
select c.id, 'Vitagen', 'vitagen.com'
from clients c where c.slug = 'apex-nutrition'
on conflict do nothing;

insert into competitors (client_id, name, domain)
select c.id, 'CoreStrength', 'corestrength.com'
from clients c where c.slug = 'apex-nutrition'
on conflict do nothing;
