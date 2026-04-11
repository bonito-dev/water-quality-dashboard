{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "4efdcf5e-7212-419f-89cd-5a311bb4c96a",
   "metadata": {},
   "outputs": [],
   "source": [
    "import time\n",
    "import wbgapi as wb\n",
    "\n",
    "def fetch_wb_with_retry(indicators, country=\"KE\", start=2000, end=2023, retries=3, delay=10):\n",
    "    for attempt in range(retries):\n",
    "        try:\n",
    "            df = wb.data.DataFrame(\n",
    "                indicators,\n",
    "                economy=country,\n",
    "                time=range(start, end + 1),\n",
    "            )\n",
    "            return df\n",
    "        except Exception as e:\n",
    "            print(f\"Attempt {attempt + 1} failed: {e}\")\n",
    "            if attempt < retries - 1:\n",
    "                print(f\"Retrying in {delay}s...\")\n",
    "                time.sleep(delay)\n",
    "    raise RuntimeError(\"World Bank API unavailable after all retries. Use CSV fallback.\")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.14.2"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
