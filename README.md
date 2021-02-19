# python-bittorrent

An almost complete implementation of the bittorrent protocol in python that uses asyncio instead of multithreading.

This project was born out of my curiosity about how torrent clients actually work and served to not just extend my knowledge on this topic, 
but introduce me to asyncio & P2P networking in python.  

## Current Status
It is almost complete (I have implemented all the core algorithms & logic), I just have to combine the parts together and test it.

## Source material
- [The original bittorrent protocol spec](http://www.bittorrent.org/beps/bep_0003.html)
- [The community bittorrent spec](https://wiki.theory.org/BitTorrentSpecification)
- [This report by Bram Cohen which outlines piece selection, seeding, and choking algorithms](http://bittorrent.org/bittorrentecon.pdf)
- [This bittorrent python implementation by borzunov for his UDP Tracker implementation (I struggled to find information on using UDP in asyncio)](https://github.com/borzunov/bit-torrent)
