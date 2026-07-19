import { describe, it, expect } from 'vitest';
import { mismoCanal, peersParaJugador, vozHabilitada, jugador, normalizarCanal } from '../voice.js';

const sala1v1 = {
  modo: '1v1',
  jugadores: [
    { id: 'a', equipo: 'A', esBot: false },
    { id: 'b', equipo: 'B', esBot: false },
  ],
};

const sala2v2 = {
  modo: '2v2',
  jugadores: [
    { id: 'a1', equipo: 'A', esBot: false },
    { id: 'a2', equipo: 'A', esBot: false },
    { id: 'b1', equipo: 'B', esBot: false },
    { id: 'b2', equipo: 'B', esBot: false },
  ],
};

const salaBot = {
  modo: '1v1-bot',
  jugadores: [
    { id: 'a', equipo: 'A', esBot: false },
    { id: 'bot', equipo: 'B', esBot: true },
  ],
};

describe('jugador', () => {
  it('encuentra al jugador por id', () => {
    expect(jugador(sala1v1, 'a')?.equipo).toBe('A');
  });
  it('devuelve null si no existe', () => {
    expect(jugador(sala1v1, 'z')).toBeNull();
  });
});

describe('normalizarCanal', () => {
  it('en 1v1 siempre publico (no hay compañeros)', () => {
    expect(normalizarCanal('equipo', '1v1')).toBe('publico');
    expect(normalizarCanal(undefined, '1v1')).toBe('publico');
  });
  it('en 2v2 default equipo, publico si se pide', () => {
    expect(normalizarCanal(undefined, '2v2')).toBe('equipo');
    expect(normalizarCanal('publico', '2v2')).toBe('publico');
    expect(normalizarCanal('otracosa', '2v2')).toBe('equipo');
  });
});

describe('mismoCanal', () => {
  it('1v1: los dos jugadores se escuchan (todo es público)', () => {
    expect(mismoCanal(sala1v1, 'a', 'b', { a: 'publico', b: 'publico' })).toBe(true);
  });
  it('2v2 canal equipo: solo compañeros', () => {
    const canales = { a1: 'equipo', a2: 'equipo', b1: 'equipo' };
    expect(mismoCanal(sala2v2, 'a1', 'a2', canales)).toBe(true);  // mismo equipo
    expect(mismoCanal(sala2v2, 'a1', 'b1', canales)).toBe(false); // rival
  });
  it('2v2 canal publico: TODOS los que estén en público se escuchan (incl. rivales)', () => {
    const canales = { a1: 'publico', b1: 'publico' };
    expect(mismoCanal(sala2v2, 'a1', 'b1', canales)).toBe(true);
  });
  it('2v2: uno en equipo y otro en publico NO se escuchan', () => {
    const canales = { a1: 'equipo', a2: 'publico' };
    expect(mismoCanal(sala2v2, 'a1', 'a2', canales)).toBe(false);
  });
  it('1v1-bot: no hay canal', () => {
    expect(mismoCanal(salaBot, 'a', 'bot', { a: 'publico', bot: 'publico' })).toBe(false);
  });
  it('nunca con uno mismo ni con un bot', () => {
    expect(mismoCanal(sala1v1, 'a', 'a', { a: 'publico' })).toBe(false);
    const s = { modo: '2v2', jugadores: [{ id: 'a1', equipo: 'A' }, { id: 'bot', equipo: 'A', esBot: true }] };
    expect(mismoCanal(s, 'a1', 'bot', { a1: 'equipo', bot: 'equipo' })).toBe(false);
  });
});

describe('peersParaJugador', () => {
  it('2v2 equipo: solo el compañero que también está en canal equipo', () => {
    const canales = { a1: 'equipo', a2: 'equipo', b1: 'equipo', b2: 'publico' };
    expect(peersParaJugador(sala2v2, 'a1', canales)).toEqual(['a2']);
  });
  it('2v2 publico: todos los humanos en público (incluye rivales)', () => {
    const canales = { a1: 'publico', a2: 'equipo', b1: 'publico', b2: 'publico' };
    expect(peersParaJugador(sala2v2, 'a1', canales).sort()).toEqual(['b1', 'b2']);
  });
  it('1v1: el rival', () => {
    expect(peersParaJugador(sala1v1, 'a', { a: 'publico', b: 'publico' })).toEqual(['b']);
  });
  it('canales vacíos → []', () => {
    expect(peersParaJugador(sala1v1, 'a', {})).toEqual([]);
    expect(peersParaJugador(sala1v1, 'a', undefined)).toEqual([]);
  });
});

describe('vozHabilitada', () => {
  it('1v1 y 2v2 habilitan voz; 1v1-bot no; sala nula no', () => {
    expect(vozHabilitada(sala1v1)).toBe(true);
    expect(vozHabilitada(sala2v2)).toBe(true);
    expect(vozHabilitada(salaBot)).toBe(false);
    expect(vozHabilitada(null)).toBe(false);
  });
});
