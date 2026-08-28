---
title: Embeddings and inference state
description: The terms this area uses - embedding, vector, source row, inference state - defined before anything relies on them.
---

This page defines the words the rest of this area uses. If you already know what
an embedding is, skip to [Inference state](#inference-state), which is the term
that is specific to Ptah.

## Embedding

An **embedding** is a list of numbers that a model produces from a piece of
input. Text goes in, a fixed-length list of numbers comes out.

The useful property is that inputs which mean similar things produce lists that
are close together, by some measure of distance. That is what makes search by
meaning possible: you turn the user's question into a list of numbers the same
way, and look for the stored lists nearest to it.

The list is called a **vector**, and the number of entries in it is its
**dimension**. A model always produces the same dimension. `bge-small-en`
produces 384 numbers; `text-embedding-3-large` produces 3072.

## Why vectors from two models cannot be mixed

Two models produce two different sets of numbers for the same text, and the
distances between them mean nothing across the two sets. Even where the
dimension happens to match, a vector from model A and a vector from model B are
not comparable — the nearest neighbors you get back are noise.

This is the fact the whole feature exists for. Changing a model is not an
adjustment to existing vectors; it is a replacement of all of them. There is no
partial state in which half your rows use the new model and half use the old one
and search still works.

## Source row and model input

The **source row** is the row your application already has: an article, a
product, a support ticket. Ptah does not invent data — it reads rows you point
it at.

The **model input** is the text Ptah builds from that row and sends to the
endpoint. You choose which columns go into it and how they are joined. Two
columns joined by a newline is a common shape:

```yaml
source:
  table: articles
  input_fields: [title, body]
preprocessing:
  separator: "\n"
```

An article whose title is `Pricing` and whose body is `We bill monthly.` becomes
the input `Pricing\nWe bill monthly.` — and it is that string, not the row, that
decides the vector.

## Inference state

**Inference state** is the persistent data an inference system reads at query
time. For this feature it means, concretely:

- the vector column on your table;
- the bookkeeping columns beside it that say which model produced each vector
  and from what;
- the vector index queries use;
- the record of which set of vectors is the active one.

It is *state*, not a service. Ptah manages the rows and columns. It does not
run a model, hold one in memory, or answer a search query.

## What changes the vectors

Anything that changes the text sent, or what turns that text into numbers,
changes every vector. That includes:

- the model, its revision, and its parameters;
- which columns are read, and the order they are joined in;
- the separator, the prefix, and the text normalization applied before sending;
- the distance metric and the stored representation.

Ptah treats that whole set as one identity — see
[Generations](../generations/), which is the next page and the idea everything
else rests on.

## What Ptah does not decide

Ptah reads the rows you name, sends the text you configured, and stores what
comes back. It has no opinion on whether the model is a good one, whether the
fields you chose are the right ones, or whether the results answer your users'
questions. Measuring that is
[retrieval evaluation](../../reference/commands/#evaluate), and it needs a set
of questions and expected answers that only you can write.
