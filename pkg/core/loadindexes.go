/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package core

import (
	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/rs/zerolog/log"

	"github.com/zinclabs/zinc/pkg/errors"
	"github.com/zinclabs/zinc/pkg/metadata"
	zincanalysis "github.com/zinclabs/zinc/pkg/uquery/analysis"
)

func LoadZincIndexesFromMetadata() error {
	indexes, err := metadata.Index.List(0, 0)
	if err != nil {
		return err
	}

	for i := range indexes {
		// cache mappings
		index := new(Index)
		index.Name = indexes[i].Name
		index.StorageType = indexes[i].StorageType
		index.DocsCount = indexes[i].DocsCount
		index.StorageSize = indexes[i].StorageSize
		index.Settings = indexes[i].Settings
		index.Mappings = indexes[i].Mappings
		index.Mappings = indexes[i].Mappings
		log.Info().Msgf("Loading index... [%s:%s]", index.Name, index.StorageType)

		// load index analysis
		if index.Settings != nil && index.Settings.Analysis != nil {
			index.Analyzers, err = zincanalysis.RequestAnalyzer(index.Settings.Analysis)
			if err != nil {
				return errors.New(errors.ErrorTypeRuntimeException, "parse stored analysis error").Cause(err)
			}
		}

		// load in memory
		ZINC_INDEX_LIST.Add(index)
	}

	return nil
}

func (index *Index) GetWriter() (*bluge.Writer, error) {
	index.lock.RLock()
	w := index.Writer
	index.lock.RUnlock()
	if w != nil {
		return w, nil
	}

	// open writer
	if err := index.openWriter(); err != nil {
		return nil, err
	}
	// update metadata
	index.UpdateMetadata()

	return index.Writer, nil
}

func (index *Index) GetReader() (*bluge.Reader, error) {
	w, err := index.GetWriter()
	if err != nil {
		return nil, err
	}
	return w.Reader()
}

func (index *Index) openWriter() error {
	var defaultSearchAnalyzer *analysis.Analyzer
	if index.Analyzers != nil {
		defaultSearchAnalyzer = index.Analyzers["default"]
	}

	var err error
	index.lock.Lock()
	index.Writer, err = OpenIndexWriter(index.Name, index.StorageType, defaultSearchAnalyzer, 0, 0)
	index.lock.Unlock()
	return err
}

func (index *Index) UpdateMetadata() {
	index.lock.RLock()
	w := index.Writer
	index.lock.RUnlock()
	if w == nil {
		return
	}

	if r, err := w.Reader(); err == nil {
		if n, err := r.Count(); err == nil {
			index.DocsCount = n
		}
	}
	status := w.Status()
	index.StorageSize = status.CurOnDiskBytes
}
